package main

// Fork-owned: `new-api link-backfill` subcommand (LINK-03). Dry run by
// default; writes only with --apply. Uses the same env (SQL_DSN, SQLITE_PATH)
// as the server. Run with the server stopped or on a replica first.

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
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
	// common.InitEnv calls flag.Parse on os.Args, which would reject our
	// subcommand flags; load only what database initialisation needs.
	_ = godotenv.Load(".env")
	if path := os.Getenv("SQLITE_PATH"); path != "" {
		common.SQLitePath = path
	}
	common.IsMasterNode = os.Getenv("NODE_TYPE") != "slave"
	if err := model.InitDB(); err != nil {
		fmt.Fprintln(stderr, "init db:", err)
		return 1
	}
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
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	report := map[string]any{"apply": *apply, "window_seconds": *window, "backfill": linked,
		"unlinkable": unlinkable, "decisions": decisions}
	if err := enc.Encode(report); err != nil {
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
