//go:build windows && (amd64 || arm64)

package windows

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/deploymenttheory/windows-mcp-server/pkg/inventory"
)

func TestLiveIndexBuildsMetadataOnlyAndPrunesSensitiveContents(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, ".ssh"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".ssh", "id_rsa"), []byte("PRIVATE KEY MUST NOT BE READ"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".env.production"), []byte("TOKEN=must-not-be-read"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "vault"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "vault", "secret.txt"), []byte("SECRET MUST NOT BE READ"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "visible.txt"), []byte("visible"), 0o644); err != nil {
		t.Fatal(err)
	}

	snapshot, stats, err := BuildIndex(context.Background(), []string{root}, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rows != len(snapshot.Rows) || stats.Rows < 4 {
		t.Fatalf("build stats rows=%d snapshot rows=%d, want boundary metadata and visible rows", stats.Rows, len(snapshot.Rows))
	}

	b, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	for _, forbidden := range []string{"PRIVATE KEY MUST NOT BE READ", "TOKEN=must-not-be-read", "SECRET MUST NOT BE READ"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("snapshot contains excluded file contents %q", forbidden)
		}
	}
	for _, expected := range []string{filepath.Join(root, ".ssh"), filepath.Join(root, ".env.production"), filepath.Join(root, "vault"), filepath.Join(root, "visible.txt")} {
		if !snapshotHasPath(snapshot, expected) {
			t.Errorf("snapshot is missing metadata row %q", expected)
		}
	}
	if len(snapshot.Exclusions) < 3 {
		t.Fatalf("exclusions=%v, want .ssh, .env*, and vault rules", snapshot.Exclusions)
	}
}

func TestLiveIndexQueryNeverHidesPossiblyStaleRoot(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "report.md")
	if err := os.WriteFile(path, []byte("report"), 0o644); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := BuildIndex(context.Background(), []string{root}, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := NewLiveIndex(context.Background(), snapshot, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	idx.MarkRootPossiblyStale(root, "watcher overflow")
	result, err := idx.Search(context.Background(), "report", "", "all", 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Coverage[0].State != IndexStatePossiblyStale {
		t.Fatalf("search coverage state=%q, want %q", result.Coverage[0].State, IndexStatePossiblyStale)
	}
	if !strings.Contains(renderLiveSearch(result), "possibly_stale") {
		t.Fatalf("search output hides stale state: %s", renderLiveSearch(result))
	}
}

func TestLiveIndexGapNeedsFullOperatorRebuild(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "report.md"), []byte("report"), 0o644); err != nil {
		t.Fatal(err)
	}
	idx, _, err := BuildLiveIndex(context.Background(), []string{root}, nil, 1000, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	idx.MarkRootPossiblyStale(root, "watcher overflow")
	if _, err := idx.Refresh(context.Background()); !errors.Is(err, errIndexNeedsFullBuild) {
		t.Fatalf("bounded refresh error=%v, want full-build requirement", err)
	}
	if _, err := idx.Rebuild(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	result, err := idx.Search(context.Background(), "report", root, "file", 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Coverage[0].State != IndexStateCurrent {
		t.Fatalf("after operator rebuild state=%q, want current", result.Coverage[0].State)
	}
}

func TestLiveIndexRefreshReconcilesTouchedFileAndWatermark(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "report.md")
	if err := os.WriteFile(path, []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	idx, _, err := BuildLiveIndex(context.Background(), []string{root}, nil, 1000, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	if err := os.WriteFile(path, []byte("one plus two"), 0o644); err != nil {
		t.Fatal(err)
	}
	idx.MarkPathChanged(path)
	before, err := idx.Search(context.Background(), "report", root, "file", 10)
	if err != nil {
		t.Fatal(err)
	}
	if before.Coverage[0].State != IndexStatePossiblyStale {
		t.Fatalf("before refresh state=%q, want possibly_stale", before.Coverage[0].State)
	}
	if _, err := idx.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, err := idx.Search(context.Background(), "report", root, "file", 10)
	if err != nil {
		t.Fatal(err)
	}
	if after.Coverage[0].State != IndexStateCurrent {
		t.Fatalf("after refresh state=%q, want current", after.Coverage[0].State)
	}
	if after.Coverage[0].Watermark != after.Coverage[0].IndexedWatermark {
		t.Fatalf("after refresh watermarks=%d/%d, want equal", after.Coverage[0].Watermark, after.Coverage[0].IndexedWatermark)
	}
	if elapsed := after.Items[0].Modified.Sub(before.Items[0].Modified); elapsed < 0 {
		t.Fatalf("refreshed metadata moved backwards: before=%s after=%s", before.Items[0].Modified, after.Items[0].Modified)
	}
}

func TestGetIndexedMetadataUsesLiveStatAndLeavesFingerprintSlotEmpty(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "note.txt")
	if err := os.WriteFile(path, []byte("before"), 0o644); err != nil {
		t.Fatal(err)
	}
	idx, _, err := BuildLiveIndex(context.Background(), []string{root}, nil, 1000, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	time.Sleep(10 * time.Millisecond)
	if err := os.WriteFile(path, []byte("after and larger"), 0o644); err != nil {
		t.Fatal(err)
	}
	metadata, err := idx.GetIndexedMetadata(path)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Size != int64(len("after and larger")) {
		t.Fatalf("metadata size=%d, want live size %d", metadata.Size, len("after and larger"))
	}
	if metadata.Fingerprint != "" {
		t.Fatalf("metadata fingerprint=%q, content fingerprinting is out of scope", metadata.Fingerprint)
	}
}

func TestIndexedMetadataToolIsReadOnlyAndRegistered(t *testing.T) {
	tool := GetIndexedMetadata()
	if !tool.IsReadOnly() {
		t.Fatal("GetIndexedMetadata must be annotated read-only")
	}
	if tool.Tool.Annotations == nil || tool.Tool.Annotations.OpenWorldHint == nil || *tool.Tool.Annotations.OpenWorldHint {
		t.Fatal("GetIndexedMetadata must not be annotated open-world")
	}
	if !snapshotHasTool(AllTools(), "GetIndexedMetadata") {
		t.Fatal("GetIndexedMetadata is missing from AllTools")
	}
}

func TestDiscoverySearchAndListExposeLiveIndexWatermarks(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "report.md")
	if err := os.WriteFile(path, []byte("report"), 0o644); err != nil {
		t.Fatal(err)
	}
	idx, _, err := BuildLiveIndex(context.Background(), []string{root}, nil, 1000, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	deps := NewBaseDeps(nil, nil, nil).WithIndexedFileIndex(idx)

	search := callTool(t, FileSearch(), deps, map[string]any{"query": "report", "scope": root, "kind": "file"})
	if search.IsError || !strings.Contains(resultText(search), "watermark=0 indexed_watermark=0") {
		t.Fatalf("live FileSearch result=%q, want current watermarks", resultText(search))
	}
	idx.MarkRootPossiblyStale(root, "watcher overflow")
	list := callTool(t, FolderOverview(), deps, map[string]any{"path": root})
	if list.IsError || !strings.Contains(resultText(list), "possibly_stale") {
		t.Fatalf("live FolderOverview result hides stale state: %q", resultText(list))
	}
}

func TestLiveIndexNativeWatcherReconcilesCreatedFile(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "baseline.txt"), []byte("baseline"), 0o644); err != nil {
		t.Fatal(err)
	}
	idx, _, err := BuildLiveIndex(context.Background(), []string{root}, nil, 1000, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	created := filepath.Join(root, "created-after-watch.txt")
	if err := os.WriteFile(created, []byte("created"), 0o644); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		result, searchErr := idx.Search(context.Background(), "created-after-watch", root, "file", 10)
		if searchErr == nil && len(result.Items) == 1 && result.Coverage[0].State == IndexStateCurrent {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	result, err := idx.Search(context.Background(), "created-after-watch", root, "file", 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Fatalf("native watcher did not reconcile created file: items=%v coverage=%v", result.Items, result.Coverage)
}

func snapshotHasPath(snapshot IndexSnapshot, want string) bool {
	want = filepath.Clean(want)
	for _, row := range snapshot.Rows {
		if filepath.Clean(row.Path) == want {
			return true
		}
	}
	return false
}

func snapshotHasTool(tools []inventory.ServerTool, name string) bool {
	for _, tool := range tools {
		if tool.Tool.Name == name {
			return true
		}
	}
	return false
}
