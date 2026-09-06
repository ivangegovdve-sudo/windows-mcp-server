//go:build windows && (amd64 || arm64)

package windows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	winapi "golang.org/x/sys/windows"

	"github.com/deploymenttheory/windows-mcp-server/pkg/inventory"
)

const (
	// DefaultLiveIndexMaxRows prevents an operator mistake from turning a
	// metadata-only index into an unbounded crawl.
	DefaultLiveIndexMaxRows = 500000
	maxLiveIndexRows        = 500000
	maxReconcileDirectories = 128
	maxReconcileRows        = 10000
	watcherBufferBytes      = 1 << 20
	indexReconcileDelay     = 150 * time.Millisecond
	liveIndexVersion        = 1
)

const (
	IndexStateCurrent       = "current"
	IndexStatePossiblyStale = "possibly_stale"
)

var errIndexNeedsFullBuild = errors.New("live index requires a full operator build after an unobserved gap")
var errIndexRowLimit = errors.New("live index row limit exceeded")

// defaultIndexExclusions are path-name rules, not content rules. The matching
// boundary itself is retained as a useful metadata row; descendants are not
// traversed. This keeps the existence of a .ssh/.env/vault boundary discoverable
// without opening anything underneath it.
var defaultIndexExclusions = []string{".ssh/**", ".env*", "vault/**"}

// IndexedMetadata is the only row stored by the live index. Fingerprint is a
// deliberately empty extension slot for a future content-aware layer; this
// layer never reads contents or computes a content hash.
type IndexedMetadata struct {
	Path        string    `json:"path"`
	Root        string    `json:"root"`
	Kind        string    `json:"kind"`
	Size        int64     `json:"size"`
	Modified    time.Time `json:"modified"`
	Fingerprint string    `json:"fingerprint,omitempty"`
}

// IndexRootStatus makes the denominator of every query explicit. A result is
// only current for the configured roots named here; it never implies a
// machine-wide or full-drive index.
type IndexRootStatus struct {
	Root             string `json:"root"`
	State            string `json:"state"`
	Reason           string `json:"reason,omitempty"`
	Watermark        uint64 `json:"watermark"`
	IndexedWatermark uint64 `json:"indexed_watermark"`
	Rows             int    `json:"rows"`
}

// IndexSnapshot is the operator-facing, metadata-only representation of an
// index baseline. It is safe to persist because it contains paths and stat
// metadata only, never file contents.
type IndexSnapshot struct {
	Version    int               `json:"version"`
	BuiltAt    time.Time         `json:"built_at"`
	Roots      []IndexRootStatus `json:"roots"`
	Exclusions []string          `json:"exclusions"`
	Rows       []IndexedMetadata `json:"rows"`
}

// IndexBuildStats is emitted by operator-facing build/acceptance commands.
type IndexBuildStats struct {
	Rows        int           `json:"rows"`
	Directories int           `json:"directories"`
	Files       int           `json:"files"`
	Duration    time.Duration `json:"duration"`
}

// IndexQueryResult carries both the answer and its coverage proof. Callers
// must render Coverage and Exclusions alongside Items; stale state is never a
// hidden implementation detail.
type IndexQueryResult struct {
	Items      []IndexedMetadata
	Coverage   []IndexRootStatus
	Exclusions []string
}

type liveIndexRoot struct {
	status         IndexRootStatus
	needsFullBuild bool
	pending        map[string]bool // path -> subtree reconcile
}

type liveIndex struct {
	mu         sync.RWMutex
	refreshMu  sync.Mutex
	rows       map[string]IndexedMetadata
	roots      map[string]*liveIndexRoot
	rootOrder  []string
	exclusions []string
	logger     *slog.Logger

	ctx      context.Context
	cancel   context.CancelFunc
	wake     chan struct{}
	events   chan watcherEvent
	gaps     chan watcherGap
	watchers []*directoryChangeWatcher
	wg       sync.WaitGroup
}

// LiveIndex is a process-local, read-only metadata index maintained by Windows
// directory notifications and a bounded reconciler. Build and refresh are
// operator operations; no MCP handler mutates the index.
type LiveIndex = liveIndex

type indexedFileIndexProvider interface {
	IndexedFileIndex() *LiveIndex
}

func indexedFileIndex(deps ToolDependencies) *LiveIndex {
	provider, ok := deps.(indexedFileIndexProvider)
	if !ok {
		return nil
	}
	return provider.IndexedFileIndex()
}

func visibleIndexedItems(deps ToolDependencies, items []IndexedMetadata) []IndexedMetadata {
	visible := items[:0]
	for _, item := range items {
		if !isProtectedRead(deps, item.Path) {
			visible = append(visible, item)
		}
	}
	return visible
}

type watcherEvent struct {
	root string
	path string
}

type watcherGap struct {
	root   string
	reason string
}

// BuildIndex performs the explicit cold build used by the operator command.
// It reads directory entries and os.FileInfo metadata only.
func BuildIndex(ctx context.Context, roots, exclusions []string, maxRows int) (IndexSnapshot, IndexBuildStats, error) {
	started := time.Now()
	normalizedRoots, err := normalizeIndexRoots(roots)
	if err != nil {
		return IndexSnapshot{}, IndexBuildStats{Duration: time.Since(started)}, err
	}
	if maxRows <= 0 {
		maxRows = DefaultLiveIndexMaxRows
	}
	if maxRows > maxLiveIndexRows {
		maxRows = maxLiveIndexRows
	}
	exclusions = normalizeIndexExclusions(exclusions)

	snapshot := IndexSnapshot{
		Version:    liveIndexVersion,
		BuiltAt:    time.Now().UTC(),
		Exclusions: exclusions,
		Rows:       make([]IndexedMetadata, 0),
	}
	stats := IndexBuildStats{}
	for _, root := range normalizedRoots {
		rows, dirs, files, err := scanIndexTree(ctx, root, root, exclusions, maxRows-len(snapshot.Rows))
		stats.Directories += dirs
		stats.Files += files
		if err != nil && ctx.Err() != nil {
			return IndexSnapshot{}, IndexBuildStats{Directories: stats.Directories, Files: stats.Files, Duration: time.Since(started)}, fmt.Errorf("build %s: %w", root, err)
		}
		if len(snapshot.Rows)+len(rows) > maxRows || errors.Is(err, errIndexRowLimit) {
			return IndexSnapshot{}, IndexBuildStats{Rows: len(snapshot.Rows) + len(rows), Directories: stats.Directories, Files: stats.Files, Duration: time.Since(started)}, fmt.Errorf("%w (%d row limit)", errIndexRowLimit, maxRows)
		}
		state := IndexStateCurrent
		reason := ""
		if err != nil {
			state = IndexStatePossiblyStale
			reason = err.Error()
		}
		snapshot.Rows = append(snapshot.Rows, rows...)
		snapshot.Roots = append(snapshot.Roots, IndexRootStatus{
			Root:             root,
			State:            state,
			Reason:           reason,
			Watermark:        0,
			IndexedWatermark: 0,
			Rows:             len(rows),
		})
	}
	sort.Slice(snapshot.Rows, func(i, j int) bool {
		return strings.ToLower(snapshot.Rows[i].Path) < strings.ToLower(snapshot.Rows[j].Path)
	})
	stats.Rows = len(snapshot.Rows)
	stats.Duration = time.Since(started)
	return snapshot, stats, nil
}

// SaveIndex writes an operator-produced snapshot atomically. The target is an
// explicit operator output path, never a path accepted by an MCP tool.
func SaveIndex(path string, snapshot IndexSnapshot) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("index output path is required")
	}
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal index: %w", err)
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create index directory: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".live-index-*.tmp")
	if err != nil {
		return fmt.Errorf("create index temporary file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("protect index temporary file: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write index: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("flush index: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close index: %w", err)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("replace index: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("install index: %w", err)
	}
	return nil
}

// LoadIndex loads an operator-produced snapshot. A loaded snapshot is a
// baseline; a long-running process should use BuildLiveIndex to establish the
// watcher before the baseline scan and close the unobserved-gap window.
func LoadIndex(path string) (IndexSnapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return IndexSnapshot{}, err
	}
	var snapshot IndexSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return IndexSnapshot{}, fmt.Errorf("decode index: %w", err)
	}
	if snapshot.Version != liveIndexVersion {
		return IndexSnapshot{}, fmt.Errorf("unsupported index version %d", snapshot.Version)
	}
	snapshot.Exclusions = normalizeIndexExclusions(snapshot.Exclusions)
	return snapshot, nil
}

// BuildLiveIndex starts notification watchers before performing the cold scan.
// Any notification received during the scan is retained as a watermark gap and
// is reconciled before a root can be reported current.
func BuildLiveIndex(ctx context.Context, roots, exclusions []string, maxRows int, logger *slog.Logger) (*LiveIndex, IndexBuildStats, error) {
	normalizedRoots, err := normalizeIndexRoots(roots)
	if err != nil {
		return nil, IndexBuildStats{}, err
	}
	exclusions = normalizeIndexExclusions(exclusions)
	idx, err := newLiveIndex(ctx, normalizedRoots, exclusions, logger)
	if err != nil {
		return nil, IndexBuildStats{}, err
	}
	snapshot, stats, buildErr := BuildIndex(ctx, normalizedRoots, exclusions, maxRows)
	idx.replaceAfterBuild(snapshot, false)
	if buildErr != nil {
		return idx, stats, buildErr
	}
	if _, err := idx.Refresh(ctx); err != nil && !errors.Is(err, errIndexNeedsFullBuild) {
		return idx, stats, err
	}
	return idx, stats, nil
}

// NewLiveIndex attaches watchers to an existing baseline. For a baseline
// produced in another process, callers should treat any time between the
// baseline and watcher attachment as a possible gap and use Rebuild. Tests and
// same-process operator flows use BuildLiveIndex instead.
func NewLiveIndex(ctx context.Context, snapshot IndexSnapshot, logger *slog.Logger) (*LiveIndex, error) {
	if snapshot.Version != liveIndexVersion {
		return nil, fmt.Errorf("unsupported index version %d", snapshot.Version)
	}
	roots := make([]string, 0, len(snapshot.Roots))
	for _, root := range snapshot.Roots {
		roots = append(roots, root.Root)
	}
	idx, err := newLiveIndex(ctx, roots, normalizeIndexExclusions(snapshot.Exclusions), logger)
	if err != nil {
		return nil, err
	}
	idx.mu.Lock()
	idx.rows = make(map[string]IndexedMetadata, len(snapshot.Rows))
	for _, row := range snapshot.Rows {
		idx.rows[indexPathKey(row.Path)] = row
	}
	for _, status := range snapshot.Roots {
		if root := idx.roots[indexPathKey(status.Root)]; root != nil {
			root.status = status
			root.pending = make(map[string]bool)
			root.status.State = IndexStatePossiblyStale
			root.status.Reason = "unobserved gap between persisted baseline and watcher attachment"
			root.needsFullBuild = true
		}
	}
	idx.recountRowsLocked()
	idx.mu.Unlock()
	return idx, nil
}

func newLiveIndex(ctx context.Context, roots, exclusions []string, logger *slog.Logger) (*LiveIndex, error) {
	if logger == nil {
		logger = slog.Default()
	}
	watchCtx, cancel := context.WithCancel(ctx)
	idx := &liveIndex{
		rows:       make(map[string]IndexedMetadata),
		roots:      make(map[string]*liveIndexRoot, len(roots)),
		rootOrder:  append([]string(nil), roots...),
		exclusions: append([]string(nil), exclusions...),
		logger:     logger,
		ctx:        watchCtx,
		cancel:     cancel,
		wake:       make(chan struct{}, 1),
		events:     make(chan watcherEvent, 4096),
		gaps:       make(chan watcherGap, len(roots)),
	}
	for _, root := range roots {
		idx.roots[indexPathKey(root)] = &liveIndexRoot{
			status:  IndexRootStatus{Root: root, State: IndexStateCurrent},
			pending: make(map[string]bool),
		}
	}
	for _, root := range roots {
		watcher, err := newDirectoryChangeWatcher(watchCtx, root, idx.events, idx.gaps)
		if err != nil {
			idx.markRootPossiblyStale(root, "watcher could not be attached: "+err.Error(), true)
			continue
		}
		idx.watchers = append(idx.watchers, watcher)
	}
	idx.wg.Add(2)
	go idx.eventLoop()
	go idx.reconcileLoop()
	return idx, nil
}

// Close stops all native directory notification handles and reconciler
// goroutines. It does not delete or rewrite an index file.
func (idx *LiveIndex) Close() error {
	if idx == nil {
		return nil
	}
	idx.cancel()
	idx.wg.Wait()
	for _, watcher := range idx.watchers {
		watcher.wg.Wait()
	}
	return nil
}

func (idx *LiveIndex) eventLoop() {
	defer idx.wg.Done()
	for {
		select {
		case event := <-idx.events:
			idx.handleEvent(event)
		case gap := <-idx.gaps:
			idx.markRootPossiblyStale(gap.root, gap.reason, true)
		case <-idx.ctx.Done():
			return
		}
	}
}

func (idx *LiveIndex) reconcileLoop() {
	defer idx.wg.Done()
	var timer *time.Timer
	var timerC <-chan time.Time
	for {
		select {
		case <-idx.wake:
			if timer == nil {
				timer = time.NewTimer(indexReconcileDelay)
			} else {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(indexReconcileDelay)
			}
			timerC = timer.C
		case <-timerC:
			_, err := idx.Refresh(idx.ctx)
			if err != nil && !errors.Is(err, errIndexNeedsFullBuild) {
				idx.logger.Warn("live index refresh failed", "error", err)
			}
			timerC = nil
		case <-idx.ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return
		}
	}
}

func (idx *LiveIndex) handleEvent(event watcherEvent) {
	root := idx.rootForPath(event.path)
	if root == "" {
		return
	}
	// Events below an excluded boundary cannot affect indexed rows. Ignoring
	// them is important: reconciling that directory would enumerate the very
	// contents the baseline deliberately pruned. The boundary event itself is
	// still reconciled through its parent so its existence remains discoverable.
	if isExcludedIndexPath(root, event.path, idx.exclusions) && !isExcludedBoundaryPath(root, event.path, idx.exclusions) {
		return
	}
	idx.mu.Lock()
	state := idx.roots[indexPathKey(root)]
	if state != nil {
		state.status.Watermark++
		state.status.State = IndexStatePossiblyStale
		state.status.Reason = "filesystem change observed; bounded refresh pending"
		state.pending[filepath.Clean(filepath.Dir(event.path))] = false
		if info, err := os.Stat(event.path); err == nil && info.IsDir() && !isExcludedIndexPath(root, event.path, idx.exclusions) {
			state.pending[filepath.Clean(event.path)] = true
		}
	}
	idx.mu.Unlock()
	idx.signalRefresh()
}

// MarkPathChanged is used by focused acceptance tests and by an operator
// adapter that has already received a native notification. It marks the root
// stale before any metadata is reconciled.
func (idx *LiveIndex) MarkPathChanged(path string) {
	root := idx.rootForPath(path)
	if root == "" {
		return
	}
	idx.handleEvent(watcherEvent{root: root, path: filepath.Clean(path)})
}

// MarkRootPossiblyStale records a known gap. Overflow and malformed/failed
// notification buffers require a full operator build; a bounded refresh must
// not claim that it repaired unknown changes.
func (idx *LiveIndex) MarkRootPossiblyStale(root, reason string) {
	// A caller using this escape hatch is declaring that the notification stream
	// is no longer a sufficient proof. Treat every such declaration as requiring
	// a full operator rebuild; a bounded refresh must not accidentally clear it.
	idx.markRootPossiblyStale(root, reason, true)
}

func (idx *LiveIndex) markRootPossiblyStale(root, reason string, fullBuild bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	state := idx.roots[indexPathKey(root)]
	if state == nil {
		return
	}
	state.status.State = IndexStatePossiblyStale
	state.status.Reason = reason
	state.needsFullBuild = state.needsFullBuild || fullBuild
}

// Refresh performs only queued directory reconciliations. It never walks an
// untouched root. A root with an overflow/unobserved-gap marker remains stale
// until Rebuild is called by an operator.
func (idx *LiveIndex) Refresh(ctx context.Context) (IndexBuildStats, error) {
	idx.refreshMu.Lock()
	defer idx.refreshMu.Unlock()
	started := time.Now()
	stats := IndexBuildStats{}
	processedDirs := 0
	processedRows := 0

	idx.mu.Lock()
	requests := make([]reconcileRequest, 0)
	startWatermarks := make(map[string]uint64, len(idx.roots))
	for _, rootPath := range idx.rootOrder {
		state := idx.roots[indexPathKey(rootPath)]
		if state == nil {
			continue
		}
		startWatermarks[indexPathKey(rootPath)] = state.status.Watermark
		for path, subtree := range state.pending {
			if processedDirs+len(requests) >= maxReconcileDirectories {
				break
			}
			requests = append(requests, reconcileRequest{root: rootPath, path: path, subtree: subtree})
			delete(state.pending, path)
		}
	}
	idx.mu.Unlock()

	for _, request := range requests {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		processedDirs++
		var rows []IndexedMetadata
		var dirs, files int
		var err error
		if request.subtree {
			rows, dirs, files, err = scanIndexTree(ctx, request.path, request.root, idx.exclusions, maxReconcileRows-processedRows)
		} else {
			rows, dirs, files, err = scanIndexDirectory(ctx, request.path, request.root, idx.exclusions)
		}
		if err != nil {
			idx.markRootPossiblyStale(request.root, "bounded refresh failed: "+err.Error(), false)
			return stats, err
		}
		processedRows += len(rows)
		if processedRows > maxReconcileRows {
			idx.markRootPossiblyStale(request.root, "bounded refresh row budget exceeded", true)
			return stats, fmt.Errorf("%w: refresh budget", errIndexRowLimit)
		}
		idx.mu.Lock()
		if request.subtree {
			idx.replaceSubtreeLocked(request.root, request.path, rows)
		} else {
			idx.mergeDirectoryLocked(request.root, request.path, rows)
		}
		idx.mu.Unlock()
		stats.Rows += len(rows)
		stats.Directories += dirs
		stats.Files += files
	}

	idx.mu.Lock()
	for _, rootPath := range idx.rootOrder {
		state := idx.roots[indexPathKey(rootPath)]
		if state == nil || state.needsFullBuild {
			continue
		}
		if len(state.pending) == 0 && state.status.Watermark == startWatermarks[indexPathKey(rootPath)] {
			state.status.State = IndexStateCurrent
			state.status.Reason = ""
			state.status.IndexedWatermark = state.status.Watermark
		} else if state.status.Watermark != state.status.IndexedWatermark {
			state.status.State = IndexStatePossiblyStale
			state.status.Reason = "notification arrived during bounded refresh"
		}
	}
	idx.recountRowsLocked()
	for _, rootPath := range idx.rootOrder {
		state := idx.roots[indexPathKey(rootPath)]
		if state != nil && state.needsFullBuild {
			idx.mu.Unlock()
			stats.Duration = time.Since(started)
			return stats, errIndexNeedsFullBuild
		}
	}
	idx.mu.Unlock()
	stats.Duration = time.Since(started)
	return stats, nil
}

// Rebuild is an explicit operator action that clears a known gap by doing a
// new cold build while notifications remain attached. No MCP tool calls it.
func (idx *LiveIndex) Rebuild(ctx context.Context, maxRows int) (IndexBuildStats, error) {
	snapshot, stats, err := BuildIndex(ctx, idx.rootOrder, idx.exclusions, maxRows)
	if err != nil {
		return stats, err
	}
	idx.replaceAfterBuild(snapshot, true)
	refreshStats, refreshErr := idx.Refresh(ctx)
	stats.Rows = refreshStats.Rows
	stats.Directories += refreshStats.Directories
	stats.Files += refreshStats.Files
	stats.Duration += refreshStats.Duration
	return stats, refreshErr
}

type reconcileRequest struct {
	root    string
	path    string
	subtree bool
}

func (idx *LiveIndex) replaceAfterBuild(snapshot IndexSnapshot, clearFullBuildGap bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	oldWatermarks := make(map[string]uint64, len(idx.roots))
	oldIndexed := make(map[string]uint64, len(idx.roots))
	oldStale := make(map[string]bool, len(idx.roots))
	oldFull := make(map[string]bool, len(idx.roots))
	for path, root := range idx.roots {
		oldWatermarks[path] = root.status.Watermark
		oldIndexed[path] = root.status.IndexedWatermark
		oldStale[path] = root.status.State == IndexStatePossiblyStale
		oldFull[path] = root.needsFullBuild
	}
	idx.rows = make(map[string]IndexedMetadata, len(snapshot.Rows))
	for _, row := range snapshot.Rows {
		idx.rows[indexPathKey(row.Path)] = row
	}
	for _, status := range snapshot.Roots {
		root := idx.roots[indexPathKey(status.Root)]
		if root == nil {
			continue
		}
		root.status = status
		if oldWatermarks[indexPathKey(status.Root)] > 0 || oldStale[indexPathKey(status.Root)] {
			root.status.Watermark = oldWatermarks[indexPathKey(status.Root)]
			root.status.IndexedWatermark = oldIndexed[indexPathKey(status.Root)]
			root.status.State = IndexStatePossiblyStale
			root.status.Reason = "notification observed during build; bounded refresh pending"
		}
		root.needsFullBuild = oldFull[indexPathKey(status.Root)] && !clearFullBuildGap
		if root.needsFullBuild {
			root.status.State = IndexStatePossiblyStale
		}
		if root.pending == nil {
			root.pending = make(map[string]bool)
		}
	}
	idx.recountRowsLocked()
}

func (idx *LiveIndex) mergeDirectoryLocked(root, dir string, rows []IndexedMetadata) {
	rootKey := indexPathKey(root)
	dir = filepath.Clean(dir)
	oldDirect := make(map[string]bool)
	for _, row := range idx.rows {
		if indexPathKey(row.Root) != rootKey || indexPathKey(filepath.Dir(row.Path)) != indexPathKey(dir) {
			continue
		}
		oldDirect[indexPathKey(row.Path)] = true
	}
	current := make(map[string]bool, len(rows))
	for _, row := range rows {
		current[indexPathKey(row.Path)] = true
		idx.rows[indexPathKey(row.Path)] = row
	}
	for key := range oldDirect {
		if current[key] {
			continue
		}
		idx.removeRowsUnderLocked(key)
	}
	for _, row := range rows {
		if row.Kind != "folder" || oldDirect[indexPathKey(row.Path)] || isExcludedIndexPath(root, row.Path, idx.exclusions) {
			continue
		}
		// A newly-created directory was scanned as a subtree by the caller when
		// the watcher event identified it. Existing directories remain bounded to
		// this immediate directory and are covered by their own notifications.
	}
}

func (idx *LiveIndex) replaceSubtreeLocked(root, path string, rows []IndexedMetadata) {
	rootKey := indexPathKey(root)
	pathKey := indexPathKey(path)
	for key, row := range idx.rows {
		if indexPathKey(row.Root) == rootKey && (key == pathKey || isPathUnder(row.Path, path)) {
			delete(idx.rows, key)
		}
	}
	for _, row := range rows {
		idx.rows[indexPathKey(row.Path)] = row
	}
}

func (idx *LiveIndex) removeRowsUnderLocked(path string) {
	for key, row := range idx.rows {
		if key == indexPathKey(path) || isPathUnder(row.Path, path) {
			delete(idx.rows, key)
		}
	}
}

func (idx *LiveIndex) recountRowsLocked() {
	counts := make(map[string]int, len(idx.roots))
	for _, row := range idx.rows {
		counts[indexPathKey(row.Root)]++
	}
	for key, root := range idx.roots {
		root.status.Rows = counts[key]
	}
}

func (idx *LiveIndex) signalRefresh() {
	select {
	case idx.wake <- struct{}{}:
	default:
	}
}

func (idx *LiveIndex) rootForPath(path string) string {
	path = filepath.Clean(path)
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	for _, root := range idx.rootOrder {
		if isPathUnderOrEqual(path, root) {
			return root
		}
	}
	return ""
}

// Snapshot returns a stable copy suitable for an operator output or diagnostic
// display. The returned rows never include content or a content fingerprint.
func (idx *LiveIndex) Snapshot() IndexSnapshot {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	snapshot := IndexSnapshot{
		Version:    liveIndexVersion,
		BuiltAt:    time.Now().UTC(),
		Exclusions: append([]string(nil), idx.exclusions...),
		Rows:       make([]IndexedMetadata, 0, len(idx.rows)),
	}
	for _, root := range idx.rootOrder {
		if state := idx.roots[indexPathKey(root)]; state != nil {
			snapshot.Roots = append(snapshot.Roots, state.status)
		}
	}
	for _, row := range idx.rows {
		snapshot.Rows = append(snapshot.Rows, row)
	}
	sort.Slice(snapshot.Rows, func(i, j int) bool {
		return strings.ToLower(snapshot.Rows[i].Path) < strings.ToLower(snapshot.Rows[j].Path)
	})
	return snapshot
}

func (idx *LiveIndex) coverage(scope string) []IndexRootStatus {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	coverage := make([]IndexRootStatus, 0, len(idx.rootOrder))
	for _, root := range idx.rootOrder {
		if scope != "" && !isPathUnderOrEqual(root, scope) && !isPathUnderOrEqual(scope, root) {
			continue
		}
		if state := idx.roots[indexPathKey(root)]; state != nil {
			coverage = append(coverage, state.status)
		}
	}
	return coverage
}

// Search searches the in-memory metadata rows by item name. The root coverage
// and exclusion rules are returned on every call, including zero-result calls.
func (idx *LiveIndex) Search(ctx context.Context, query, scope, kind string, limit int) (IndexQueryResult, error) {
	if err := ctx.Err(); err != nil {
		return IndexQueryResult{}, err
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return IndexQueryResult{}, errors.New("query is required")
	}
	if kind != "all" && kind != "file" && kind != "folder" {
		return IndexQueryResult{}, fmt.Errorf("kind must be one of all, file, folder")
	}
	if limit < 1 {
		return IndexQueryResult{}, errors.New("limit must be at least 1")
	}
	if limit > maxIndexedSearchResults {
		limit = maxIndexedSearchResults
	}
	coverage := idx.coverage(scope)
	if scope != "" && len(coverage) == 0 {
		return IndexQueryResult{}, fmt.Errorf("scope %s is outside configured index roots", scope)
	}
	idx.mu.RLock()
	items := make([]IndexedMetadata, 0)
	needle := strings.ToLower(query)
	for _, row := range idx.rows {
		if scope != "" && !isPathUnderOrEqual(row.Path, scope) {
			continue
		}
		if kind != "all" && row.Kind != kind {
			continue
		}
		if !strings.Contains(strings.ToLower(filepath.Base(row.Path)), needle) {
			continue
		}
		items = append(items, row)
	}
	idx.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool { return strings.ToLower(items[i].Path) < strings.ToLower(items[j].Path) })
	if len(items) > limit {
		items = items[:limit]
	}
	return IndexQueryResult{Items: items, Coverage: coverage, Exclusions: append([]string(nil), idx.exclusions...)}, nil
}

// List lists immediate children from the metadata index and reports the same
// coverage watermarks as Search. It never performs a direct fallback walk.
func (idx *LiveIndex) List(ctx context.Context, path string, maxEntries int) (IndexQueryResult, error) {
	if err := ctx.Err(); err != nil {
		return IndexQueryResult{}, err
	}
	if maxEntries < 1 {
		return IndexQueryResult{}, errors.New("max_entries must be at least 1")
	}
	if maxEntries > maxOverviewEntries {
		maxEntries = maxOverviewEntries
	}
	coverage := idx.coverage(path)
	if path != "" && len(coverage) == 0 {
		return IndexQueryResult{}, fmt.Errorf("path %s is outside configured index roots", path)
	}
	idx.mu.RLock()
	items := make([]IndexedMetadata, 0)
	for _, row := range idx.rows {
		if path == "" {
			for _, root := range idx.rootOrder {
				if indexPathKey(row.Root) == indexPathKey(root) && indexPathKey(row.Path) == indexPathKey(root) {
					items = append(items, row)
				}
			}
			continue
		}
		if indexPathKey(filepath.Dir(row.Path)) == indexPathKey(path) {
			items = append(items, row)
		}
	}
	idx.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool {
		if items[i].Kind != items[j].Kind {
			return items[i].Kind == "folder"
		}
		return strings.ToLower(items[i].Path) < strings.ToLower(items[j].Path)
	})
	if len(items) > maxEntries {
		items = items[:maxEntries]
	}
	return IndexQueryResult{Items: items, Coverage: coverage, Exclusions: append([]string(nil), idx.exclusions...)}, nil
}

// GetIndexedMetadata is intentionally live: its fields come from os.Stat at
// call time, while the returned root coverage still tells the caller whether
// the indexed search/list view is current.
func (idx *LiveIndex) GetIndexedMetadata(path string) (IndexedMetadata, error) {
	root := idx.rootForPath(path)
	if root == "" {
		return IndexedMetadata{}, fmt.Errorf("path %s is outside configured index roots", path)
	}
	info, err := os.Stat(path)
	if err != nil {
		return IndexedMetadata{}, err
	}
	kind := "file"
	if info.IsDir() {
		kind = "folder"
	}
	return IndexedMetadata{
		Path:     filepath.Clean(path),
		Root:     root,
		Kind:     kind,
		Size:     info.Size(),
		Modified: info.ModTime(),
		// Fingerprint intentionally remains empty. Layer two may attach a
		// content identity here after an explicit, separately governed design.
	}, nil
}

func renderIndexCoverage(b *strings.Builder, coverage []IndexRootStatus, exclusions []string) {
	b.WriteString("Coverage (configured roots only):\n")
	if len(coverage) == 0 {
		b.WriteString("  (none)\n")
	}
	for _, root := range coverage {
		fmt.Fprintf(b, "  %s [%s] rows=%d watermark=%d indexed_watermark=%d",
			root.Root, root.State, root.Rows, root.Watermark, root.IndexedWatermark)
		if root.Reason != "" {
			fmt.Fprintf(b, " reason=%s", root.Reason)
		}
		b.WriteByte('\n')
	}
	b.WriteString("Exclusions (boundary metadata retained; contents never read):\n")
	for _, exclusion := range exclusions {
		fmt.Fprintf(b, "  %s\n", exclusion)
	}
}

func renderLiveSearch(result IndexQueryResult) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Live indexed metadata search (%d result(s)):\n", len(result.Items))
	renderIndexCoverage(&b, result.Coverage, result.Exclusions)
	b.WriteString("Results:\n")
	for _, item := range result.Items {
		fmt.Fprintf(&b, "  [%s] %s — %d bytes — modified %s\n", item.Kind, item.Path, item.Size, item.Modified.Format(time.RFC3339Nano))
	}
	return strings.TrimSuffix(b.String(), "\n")
}

func renderLiveList(path string, result IndexQueryResult) string {
	var b strings.Builder
	if path == "" {
		path = "configured roots"
	}
	fmt.Fprintf(&b, "Live indexed list for %s (%d immediate result(s)):\n", path, len(result.Items))
	renderIndexCoverage(&b, result.Coverage, result.Exclusions)
	b.WriteString("Entries:\n")
	for _, item := range result.Items {
		fmt.Fprintf(&b, "  [%s] %s — %d bytes — modified %s\n", item.Kind, item.Path, item.Size, item.Modified.Format(time.RFC3339Nano))
	}
	return strings.TrimSuffix(b.String(), "\n")
}

func renderLiveMetadata(metadata IndexedMetadata, coverage []IndexRootStatus, exclusions []string) string {
	var b strings.Builder
	b.WriteString("Live indexed metadata (source: os.Stat):\n")
	fmt.Fprintf(&b, "  Path: %s\n  Root: %s\n  Type: %s\n  Size: %d bytes\n  Modified: %s\n  Fingerprint: not collected (reserved for a future content-aware layer)\n", metadata.Path, metadata.Root, metadata.Kind, metadata.Size, metadata.Modified.Format(time.RFC3339Nano))
	renderIndexCoverage(&b, coverage, exclusions)
	return strings.TrimSuffix(b.String(), "\n")
}

// GetIndexedMetadata exposes only live metadata and is read-only. It is
// intentionally unavailable unless the operator configured a live index root.
func GetIndexedMetadata() inventory.ServerTool {
	destructive, openWorld := false, false
	return NewToolFromHandler(
		ToolsetFilesystem,
		mcp.Tool{
			Name: "GetIndexedMetadata",
			Description: "Return live os.Stat metadata for a path inside a configured live-index root. " +
				"Search/list watermark state, roots, exclusions, and the empty future fingerprint slot are shown; file contents are never read.",
			Annotations: &mcp.ToolAnnotations{Title: "Live indexed metadata", ReadOnlyHint: true, DestructiveHint: &destructive, OpenWorldHint: &openWorld},
			InputSchema: &jsonschema.Schema{Type: "object", Properties: map[string]*jsonschema.Schema{
				"path": {Type: "string", Description: "Local file or folder path. Relative paths resolve against Desktop."},
			}, Required: []string{"path"}},
		},
		func(_ context.Context, deps ToolDependencies, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args, err := ArgsMap(req)
			if err != nil {
				return NewToolResultError(err.Error()), nil
			}
			rawPath, err := RequiredString(args, "path")
			if err != nil {
				return NewToolResultError(err.Error()), nil
			}
			path, err := resolveLocalAnyPath(rawPath)
			if err != nil {
				return NewToolResultError(err.Error()), nil
			}
			if r := checkProtected(deps, path, false); r != nil {
				return r, nil
			}
			idx := indexedFileIndex(deps)
			if idx == nil {
				return NewToolResultError("live index is not configured; start the server with --index-root"), nil
			}
			metadata, err := idx.GetIndexedMetadata(path)
			if err != nil {
				return NewToolResultErrorFromErr("live metadata failed", err), nil
			}
			return NewToolResultText(renderLiveMetadata(metadata, idx.coverage(path), idx.exclusions)), nil
		},
	)
}

func resolveLocalAnyPath(raw string) (string, error) {
	path, err := resolvePath(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	if isUNCPath(path) {
		return "", errors.New("UNC paths are not supported by local discovery tools")
	}
	driveType, err := driveTypeForPath(path)
	if err != nil {
		return "", err
	}
	if driveType == winapi.DRIVE_REMOTE {
		return "", errors.New("network-mapped paths are not supported by local discovery tools")
	}
	return filepath.Clean(path), nil
}

func normalizeIndexRoots(roots []string) ([]string, error) {
	if len(roots) == 0 {
		return nil, errors.New("at least one --root is required")
	}
	seen := make(map[string]bool, len(roots))
	out := make([]string, 0, len(roots))
	for _, raw := range roots {
		path, err := resolveLocalFolder(raw)
		if err != nil {
			return nil, err
		}
		key := indexPathKey(path)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, path)
	}
	sort.Slice(out, func(i, j int) bool { return strings.ToLower(out[i]) < strings.ToLower(out[j]) })
	return out, nil
}

func normalizeIndexExclusions(exclusions []string) []string {
	if len(exclusions) == 0 {
		exclusions = defaultIndexExclusions
	}
	seen := make(map[string]bool, len(exclusions))
	out := make([]string, 0, len(exclusions))
	for _, exclusion := range exclusions {
		exclusion = strings.TrimSpace(strings.ReplaceAll(exclusion, "\\", "/"))
		if exclusion == "" {
			continue
		}
		exclusion = strings.ToLower(exclusion)
		if seen[exclusion] {
			continue
		}
		seen[exclusion] = true
		out = append(out, exclusion)
	}
	sort.Strings(out)
	return out
}

func indexPathKey(path string) string { return strings.ToLower(filepath.Clean(path)) }

func isPathUnderOrEqual(path, parent string) bool {
	path = filepath.Clean(path)
	parent = filepath.Clean(parent)
	return indexPathKey(path) == indexPathKey(parent) || isPathUnder(path, parent)
}

func isPathUnder(path, parent string) bool {
	path = filepath.Clean(path)
	parent = filepath.Clean(parent)
	return strings.HasPrefix(indexPathKey(path), indexPathKey(parent)+string(filepath.Separator))
}

func isExcludedIndexPath(root, path string, exclusions []string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return false
	}
	parts := strings.Split(filepath.ToSlash(rel), "/")
	for _, part := range parts {
		part = strings.ToLower(part)
		for _, rule := range exclusions {
			base := strings.TrimSuffix(rule, "/**")
			if strings.HasSuffix(base, "*") {
				if strings.HasPrefix(part, strings.TrimSuffix(base, "*")) {
					return true
				}
			} else if part == base {
				return true
			}
		}
	}
	return false
}

func isExcludedBoundaryPath(root, path string, exclusions []string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return false
	}
	parts := strings.Split(filepath.ToSlash(rel), "/")
	if len(parts) == 0 {
		return false
	}
	last := strings.ToLower(parts[len(parts)-1])
	for _, rule := range exclusions {
		base := strings.TrimSuffix(rule, "/**")
		if strings.HasSuffix(base, "*") {
			if strings.HasPrefix(last, strings.TrimSuffix(base, "*")) {
				return true
			}
		} else if last == base {
			return true
		}
	}
	return false
}

func scanIndexTree(ctx context.Context, path, root string, exclusions []string, maxRows int) ([]IndexedMetadata, int, int, error) {
	rows := make([]IndexedMetadata, 0)
	dirs, files := 0, 0
	var firstErr error
	err := filepath.WalkDir(path, func(current string, entry fs.DirEntry, walkErr error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if walkErr != nil {
			if firstErr == nil {
				firstErr = walkErr
			}
			return nil
		}
		if len(rows) >= maxRows {
			return errIndexRowLimit
		}
		if current != root && isExcludedIndexPath(root, current, exclusions) && current != filepath.Clean(root) {
			row, err := indexMetadataFromDirEntry(current, root, entry)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return nil
			}
			rows = append(rows, row)
			if entry.IsDir() {
				return fs.SkipDir
			}
			if row.Kind == "folder" {
				dirs++
			} else {
				files++
			}
			return nil
		}
		row, err := indexMetadataFromDirEntry(current, root, entry)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			return nil
		}
		rows = append(rows, row)
		if row.Kind == "folder" {
			dirs++
		} else {
			files++
		}
		if entry.Type()&os.ModeSymlink != 0 && entry.IsDir() {
			return fs.SkipDir
		}
		return nil
	})
	if err != nil && !errors.Is(err, errIndexRowLimit) {
		return rows, dirs, files, err
	}
	if firstErr != nil {
		return rows, dirs, files, firstErr
	}
	return rows, dirs, files, err
}

func scanIndexDirectory(ctx context.Context, path, root string, exclusions []string) ([]IndexedMetadata, int, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, 0, err
	}
	_ = exclusions // exclusion boundaries are represented; subtree scans prune their descendants.
	entries, err := os.ReadDir(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, 0, nil
		}
		return nil, 0, 0, err
	}
	rows := make([]IndexedMetadata, 0, len(entries))
	dirs, files := 0, 0
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, dirs, files, err
		}
		current := filepath.Join(path, entry.Name())
		row, err := indexMetadataFromDirEntry(current, root, entry)
		if err != nil {
			return nil, dirs, files, err
		}
		rows = append(rows, row)
		if row.Kind == "folder" {
			dirs++
		} else {
			files++
		}
	}
	return rows, dirs, files, nil
}

func indexMetadataFromDirEntry(path, root string, entry fs.DirEntry) (IndexedMetadata, error) {
	info, err := entry.Info()
	if err != nil {
		return IndexedMetadata{}, err
	}
	kind := "file"
	if info.IsDir() {
		kind = "folder"
	}
	return IndexedMetadata{Path: filepath.Clean(path), Root: filepath.Clean(root), Kind: kind, Size: info.Size(), Modified: info.ModTime()}, nil
}

// directoryChangeWatcher is a small direct ReadDirectoryChangesW adapter. A
// recursive handle per configured root means newly-created subdirectories are
// covered immediately; overflow, malformed records, and any completion error
// are emitted as a gap rather than guessed around.
type directoryChangeWatcher struct {
	root   string
	handle winapi.Handle
	cancel context.CancelFunc
	ready  chan error
	wg     sync.WaitGroup
}

func newDirectoryChangeWatcher(ctx context.Context, root string, events chan<- watcherEvent, gaps chan<- watcherGap) (*directoryChangeWatcher, error) {
	path, err := winapi.UTF16PtrFromString(root)
	if err != nil {
		return nil, err
	}
	handle, err := winapi.CreateFile(path, winapi.FILE_LIST_DIRECTORY,
		winapi.FILE_SHARE_READ|winapi.FILE_SHARE_WRITE|winapi.FILE_SHARE_DELETE,
		nil, winapi.OPEN_EXISTING, winapi.FILE_FLAG_BACKUP_SEMANTICS|winapi.FILE_FLAG_OVERLAPPED, 0)
	if err != nil {
		return nil, err
	}
	watchCtx, cancel := context.WithCancel(ctx)
	w := &directoryChangeWatcher{root: root, handle: handle, cancel: cancel, ready: make(chan error, 1)}
	w.wg.Add(1)
	go w.run(watchCtx, events, gaps)
	select {
	case err := <-w.ready:
		if err != nil {
			w.wg.Wait()
			return nil, err
		}
		return w, nil
	case <-ctx.Done():
		w.cancel()
		w.wg.Wait()
		return nil, ctx.Err()
	}
}

func (w *directoryChangeWatcher) run(ctx context.Context, events chan<- watcherEvent, gaps chan<- watcherGap) {
	defer w.wg.Done()
	defer w.cancel()
	defer func() { _ = winapi.CloseHandle(w.handle) }()
	buffer := make([]byte, watcherBufferBytes)
	mask := uint32(winapi.FILE_NOTIFY_CHANGE_FILE_NAME | winapi.FILE_NOTIFY_CHANGE_DIR_NAME |
		winapi.FILE_NOTIFY_CHANGE_SIZE | winapi.FILE_NOTIFY_CHANGE_LAST_WRITE |
		winapi.FILE_NOTIFY_CHANGE_CREATION)
	ready := w.ready
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		overlapped := new(winapi.Overlapped)
		var returned uint32
		err := winapi.ReadDirectoryChanges(w.handle, &buffer[0], uint32(len(buffer)), true, mask, &returned, overlapped, 0)
		if ready != nil {
			ready <- func() error {
				if err != nil && !errors.Is(err, winapi.ERROR_IO_PENDING) {
					return err
				}
				return nil
			}()
			ready = nil
		}
		if err != nil && !errors.Is(err, winapi.ERROR_IO_PENDING) {
			sendWatcherGap(ctx, gaps, w.root, "ReadDirectoryChangesW failed: "+err.Error())
			return
		}
		if err := waitDirectoryChange(ctx, w.handle, overlapped, &returned); err != nil {
			if errors.Is(err, winapi.ERROR_OPERATION_ABORTED) && ctx.Err() != nil {
				return
			}
			sendWatcherGap(ctx, gaps, w.root, "directory notification completion failed: "+err.Error())
			return
		}
		if returned == 0 {
			sendWatcherGap(ctx, gaps, w.root, "watcher overflow or unobserved gap: zero-byte notification buffer")
			return
		}
		paths, parseErr := parseDirectoryChangeBuffer(buffer[:returned], w.root)
		if parseErr != nil {
			sendWatcherGap(ctx, gaps, w.root, "watcher overflow or unobserved gap: "+parseErr.Error())
			return
		}
		for _, path := range paths {
			select {
			case events <- watcherEvent{root: w.root, path: path}:
			case <-ctx.Done():
				return
			default:
				sendWatcherGap(ctx, gaps, w.root, "watcher event channel overflow; events were not observed")
				return
			}
		}
	}
}

// waitDirectoryChange polls the overlapped completion so cancellation is
// observable even on hosts where CancelIoEx does not wake a blocking
// GetOverlappedResult call promptly.
func waitDirectoryChange(ctx context.Context, handle winapi.Handle, overlapped *winapi.Overlapped, returned *uint32) error {
	cancelled := false
	for {
		if ctx.Err() != nil && !cancelled {
			_ = winapi.CancelIoEx(handle, overlapped)
			cancelled = true
		}
		err := winapi.GetOverlappedResult(handle, overlapped, returned, false)
		if err == nil {
			return nil
		}
		if !errors.Is(err, winapi.ERROR_IO_INCOMPLETE) {
			return err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
	}
}

func sendWatcherGap(ctx context.Context, gaps chan<- watcherGap, root, reason string) {
	select {
	case gaps <- watcherGap{root: root, reason: reason}:
	case <-ctx.Done():
	default:
		// A previously queued gap is already sufficient to force a full build.
	}
}

func parseDirectoryChangeBuffer(buffer []byte, root string) ([]string, error) {
	paths := make([]string, 0)
	for offset := uint32(0); offset < uint32(len(buffer)); {
		if uint32(len(buffer))-offset < uint32(unsafe.Sizeof(winapi.FileNotifyInformation{})) {
			return nil, errors.New("truncated FILE_NOTIFY_INFORMATION header")
		}
		raw := (*winapi.FileNotifyInformation)(unsafe.Pointer(&buffer[offset]))
		nameBytes := raw.FileNameLength
		headerBytes := uint32(unsafe.Offsetof(raw.FileName))
		if nameBytes%2 != 0 || headerBytes+nameBytes > uint32(len(buffer))-offset {
			return nil, errors.New("invalid FILE_NOTIFY_INFORMATION name length")
		}
		name := winapi.UTF16ToString(unsafe.Slice(&raw.FileName, nameBytes/2))
		if name == "" {
			return nil, errors.New("empty FILE_NOTIFY_INFORMATION name")
		}
		paths = append(paths, filepath.Clean(filepath.Join(root, name)))
		if raw.NextEntryOffset == 0 {
			break
		}
		if raw.NextEntryOffset < headerBytes+nameBytes || offset+raw.NextEntryOffset >= uint32(len(buffer)) {
			return nil, errors.New("invalid FILE_NOTIFY_INFORMATION next offset")
		}
		offset += raw.NextEntryOffset
	}
	return paths, nil
}
