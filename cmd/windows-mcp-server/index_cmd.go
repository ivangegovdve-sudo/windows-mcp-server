//go:build windows && (amd64 || arm64)

package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/deploymenttheory/windows-mcp-server/pkg/windows"
)

func indexCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "index",
		Short: "Build and refresh the operator-managed live metadata index",
		Long: "Build and refresh a metadata-only index for explicitly named local roots. " +
			"These commands are operator-facing; the MCP surface has read-only search, list, and live-stat tools only.",
	}
	cmd.AddCommand(indexBuildCmd(), indexRefreshCmd())
	return cmd
}

func indexBuildCmd() *cobra.Command {
	var roots []string
	var output string
	var maxRows int
	cmd := &cobra.Command{
		Use:   "build",
		Short: "Perform an explicit cold metadata build",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			started := time.Now()
			snapshot, stats, err := windows.BuildIndex(ctx, roots, nil, maxRows)
			if err != nil {
				return err
			}
			if output == "" {
				output = defaultLiveIndexPath()
			}
			if err := windows.SaveIndex(output, snapshot); err != nil {
				return err
			}
			printIndexReport(cmd, "cold_build", output, snapshot, stats, time.Since(started))
			return nil
		},
	}
	cmd.Flags().StringSliceVar(&roots, "root", nil, "Local folder root to index; repeat for multiple roots (required).")
	cmd.Flags().StringVar(&output, "out", "", "Index JSON output path (default: %LOCALAPPDATA%\\windows-mcp\\live-index.json).")
	cmd.Flags().IntVar(&maxRows, "max-rows", windows.DefaultLiveIndexMaxRows, "Maximum metadata rows.")
	_ = cmd.MarkFlagRequired("root")
	return cmd
}

func indexRefreshCmd() *cobra.Command {
	var indexPath string
	var maxRows int
	cmd := &cobra.Command{
		Use:   "refresh",
		Short: "Rebuild an existing baseline with notifications attached",
		Long: "Refresh is an explicit operator baseline operation. It starts the Windows directory " +
			"notification watcher before the scan, then writes the resulting metadata snapshot. The MCP " +
			"server uses the bounded reconciler for observed changes while it is running.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if strings.TrimSpace(indexPath) == "" {
				return fmt.Errorf("--index is required")
			}
			snapshot, err := windows.LoadIndex(indexPath)
			if err != nil {
				return err
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			started := time.Now()
			roots := make([]string, 0, len(snapshot.Roots))
			for _, root := range snapshot.Roots {
				roots = append(roots, root.Root)
			}
			live, stats, err := windows.BuildLiveIndex(ctx, roots, snapshot.Exclusions, maxRows, nil)
			if live != nil {
				defer live.Close()
			}
			if err != nil {
				return err
			}
			refreshed := live.Snapshot()
			if err := windows.SaveIndex(indexPath, refreshed); err != nil {
				return err
			}
			printIndexReport(cmd, "operator_refresh", indexPath, refreshed, stats, time.Since(started))
			return nil
		},
	}
	cmd.Flags().StringVar(&indexPath, "index", "", "Existing index JSON path to refresh (required).")
	cmd.Flags().IntVar(&maxRows, "max-rows", windows.DefaultLiveIndexMaxRows, "Maximum metadata rows.")
	_ = cmd.MarkFlagRequired("index")
	return cmd
}

func defaultLiveIndexPath() string {
	base := os.Getenv("LOCALAPPDATA")
	if base == "" {
		base = filepath.Join(os.TempDir(), "windows-mcp")
	}
	return filepath.Join(base, "windows-mcp", "live-index.json")
}

func printIndexReport(cmd *cobra.Command, operation, output string, snapshot windows.IndexSnapshot, stats windows.IndexBuildStats, elapsed time.Duration) {
	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "%s complete\n", operation)
	fmt.Fprintf(out, "output=%s\nrows=%d\ndirectories=%d\nfiles=%d\nelapsed=%s\n", output, len(snapshot.Rows), stats.Directories, stats.Files, elapsed)
	fmt.Fprintln(out, "roots:")
	for _, root := range snapshot.Roots {
		fmt.Fprintf(out, "  %s [%s] rows=%d watermark=%d indexed_watermark=%d\n", root.Root, root.State, root.Rows, root.Watermark, root.IndexedWatermark)
	}
	fmt.Fprintln(out, "exclusions:")
	for _, exclusion := range snapshot.Exclusions {
		fmt.Fprintf(out, "  %s\n", exclusion)
	}
}
