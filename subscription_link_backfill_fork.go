package main

// Fork-owned: `new-api link-backfill` subcommand (LINK-03). Dry run by
// default; writes only with --apply. Uses the same env (SQL_DSN, SQLITE_PATH)
// as the server. Run with the server stopped or on a replica first.

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func runLinkBackfill(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("link-backfill", flag.ContinueOnError)
	fs.SetOutput(stderr)
	apply := fs.Bool("apply", false, "write the decisions (default: dry run)")
	window := fs.Int64("window", 300, "max seconds between order completion and subscription start")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *window < 0 || fs.NArg() != 0 {
		fmt.Fprintln(stderr, "window must be non-negative and positional arguments are not supported")
		return 2
	}
	// This subcommand emits one JSON report on stdout. Database startup
	// diagnostics belong on stderr, as they do for other command failures.
	common.LogWriterMu.Lock()
	previousWriter, previousErrorWriter := gin.DefaultWriter, gin.DefaultErrorWriter
	gin.DefaultWriter, gin.DefaultErrorWriter = stderr, stderr
	common.LogWriterMu.Unlock()
	defer func() {
		common.LogWriterMu.Lock()
		gin.DefaultWriter, gin.DefaultErrorWriter = previousWriter, previousErrorWriter
		common.LogWriterMu.Unlock()
	}()
	// common.InitEnv calls flag.Parse on os.Args, which would reject our
	// subcommand flags; load only what database initialisation needs.
	_ = godotenv.Load(".env")
	if path := os.Getenv("SQLITE_PATH"); path != "" {
		common.SQLitePath = path
	}
	if err := model.InitSubscriptionLinkBackfillDB(*apply); err != nil {
		fmt.Fprintln(stderr, "init db:", err)
		return 1
	}
	sqlDB, err := model.DB.DB()
	if err != nil {
		fmt.Fprintln(stderr, "open db:", err)
		return 1
	}
	defer sqlDB.Close()
	decisions, err := model.PlanSubscriptionLinkBackfill(*window)
	if err != nil {
		fmt.Fprintln(stderr, "plan:", err)
		return 1
	}
	linked, unlinkable := 0, 0
	for _, d := range decisions {
		if d.Result == model.LinkSourceBackfill {
			linked++
		} else {
			unlinkable++
		}
	}
	report := map[string]any{"apply": *apply, "window_seconds": *window, "backfill": linked,
		"unlinkable": unlinkable, "decisions": decisions}
	data, err := common.Marshal(report)
	if err == nil {
		data, err = common.IndentJson(data)
	}
	if err == nil {
		_, err = fmt.Fprintln(stdout, string(data))
	}
	if err != nil {
		fmt.Fprintln(stderr, "write report:", err)
		return 1
	}
	if *apply {
		if err := model.ApplySubscriptionLinkBackfill(decisions); err != nil {
			fmt.Fprintln(stderr, "apply:", err)
			return 1
		}
		fmt.Fprintf(stderr, "applied: %d linked, %d marked unlinkable\n", linked, unlinkable)
	}
	return 0
}
