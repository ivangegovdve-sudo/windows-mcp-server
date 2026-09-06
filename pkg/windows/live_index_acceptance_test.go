//go:build windows && (amd64 || arm64)

package windows

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestLiveIndexAcceptanceDOutput is the real-host acceptance run. It is opt-in
// because it watches and temporarily changes metadata on a user-selected root.
// Run with WINDOWS_MCP_LIVE_INDEX_ACCEPTANCE=1; the root defaults to D:\output
// and can be overridden with WINDOWS_MCP_INDEX_ROOT.
func TestLiveIndexAcceptanceDOutput(t *testing.T) {
	if os.Getenv("WINDOWS_MCP_LIVE_INDEX_ACCEPTANCE") != "1" {
		t.Skip("set WINDOWS_MCP_LIVE_INDEX_ACCEPTANCE=1 for the real Windows acceptance run")
	}
	root := os.Getenv("WINDOWS_MCP_INDEX_ROOT")
	if root == "" {
		root = `D:\output`
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	idx, buildStats, err := BuildLiveIndex(ctx, []string{root}, nil, DefaultLiveIndexMaxRows, nil)
	if err != nil {
		t.Fatal(err)
	}

	noChangeStarted := time.Now()
	noChangeStats, err := idx.Refresh(ctx)
	if err != nil {
		t.Fatal(err)
	}
	noChangeElapsed := time.Since(noChangeStarted)

	snapshot := idx.Snapshot()
	if len(snapshot.Roots) != 1 {
		t.Fatalf("roots=%v, want one configured root", snapshot.Roots)
	}
	markdownCount, markdownBytes := 0, int64(0)
	var touched IndexedMetadata
	for _, row := range snapshot.Rows {
		if row.Kind != "file" {
			continue
		}
		if strings.EqualFold(filepath.Ext(row.Path), ".md") {
			markdownCount++
			markdownBytes += row.Size
			if touched.Path == "" && !strings.HasPrefix(strings.ToLower(filepath.Base(row.Path)), ".env") {
				touched = row
			}
		}
	}
	if touched.Path == "" {
		t.Fatal("no markdown file available for the touched-file acceptance demonstration")
	}

	pattern := strings.TrimSuffix(filepath.Base(touched.Path), filepath.Ext(touched.Path))
	searchStarted := time.Now()
	search, err := idx.Search(ctx, pattern, root, "file", 10)
	if err != nil {
		t.Fatal(err)
	}
	searchElapsed := time.Since(searchStarted)
	if searchElapsed >= time.Second {
		t.Fatalf("pattern search took %s, want under one second", searchElapsed)
	}

	info, err := os.Stat(touched.Path)
	if err != nil {
		t.Fatal(err)
	}
	originalAccess, originalModified := info.ModTime(), info.ModTime()
	defer func() {
		// Stop notifications before restoring the test's metadata so the
		// cleanup write cannot create a post-acceptance stale event.
		_ = idx.Close()
		_ = os.Chtimes(touched.Path, originalAccess, originalModified)
	}()
	newModified := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(touched.Path, newModified, newModified); err != nil {
		t.Fatal(err)
	}

	var before IndexQueryResult
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		before, err = idx.Search(ctx, pattern, root, "file", 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(before.Coverage) == 1 && before.Coverage[0].State == IndexStatePossiblyStale {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if len(before.Coverage) != 1 || before.Coverage[0].State != IndexStatePossiblyStale {
		t.Fatalf("touched-file state before refresh=%v, want possibly_stale", before.Coverage)
	}

	refreshStarted := time.Now()
	refreshStats, err := idx.Refresh(ctx)
	if err != nil {
		t.Fatal(err)
	}
	after, err := idx.Search(ctx, pattern, root, "file", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Coverage) != 1 || after.Coverage[0].State != IndexStateCurrent {
		t.Fatalf("touched-file state after refresh=%v, want current", after.Coverage)
	}
	var sameRow *IndexedMetadata
	for i := range after.Items {
		if after.Items[i].Path == touched.Path {
			sameRow = &after.Items[i]
			break
		}
	}
	if sameRow == nil {
		t.Fatalf("touched row %q missing after refresh", touched.Path)
	}

	t.Logf("ACCEPTANCE root=%s", root)
	t.Logf("ACCEPTANCE cold_build=%s rows=%d", buildStats.Duration, len(snapshot.Rows))
	t.Logf("ACCEPTANCE no_change_refresh=%s measured=%s rows_scanned=%d", noChangeStats.Duration, noChangeElapsed, noChangeStats.Rows)
	t.Logf("ACCEPTANCE markdown_count=%d markdown_total_bytes=%d", markdownCount, markdownBytes)
	t.Logf("ACCEPTANCE pattern=%q search=%s results=%d", pattern, searchElapsed, len(search.Items))
	t.Logf("ACCEPTANCE touched_before path=%s state=%s watermark=%d indexed_watermark=%d", touched.Path, before.Coverage[0].State, before.Coverage[0].Watermark, before.Coverage[0].IndexedWatermark)
	t.Logf("ACCEPTANCE touched_after path=%s state=%s watermark=%d indexed_watermark=%d size=%d modified=%s refresh=%s rows_scanned=%d", sameRow.Path, after.Coverage[0].State, after.Coverage[0].Watermark, after.Coverage[0].IndexedWatermark, sameRow.Size, sameRow.Modified.Format(time.RFC3339Nano), time.Since(refreshStarted), refreshStats.Rows)
	t.Logf("ACCEPTANCE exclusions=%s", strings.Join(snapshot.Exclusions, ","))
}
