package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/service"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

// list is read-only; retry applies only a previously persisted final intent.
// There is deliberately no command that guesses the result of an upstream call.
func runBillingRecovery(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("billing-recovery", flag.ContinueOnError)
	fs.SetOutput(stderr)
	retry := fs.Bool("retry", false, "retry persisted settlement/refund intents (default: list unresolved)")
	requestID := fs.String("request-id", "", "retry one known intent; requires --retry")
	limit := fs.Int("limit", 100, "maximum sessions (1..1000)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 || *limit < 1 || *limit > 1000 || (!*retry && *requestID != "") {
		fmt.Fprintln(stderr, "invalid arguments: limit must be 1..1000; request-id requires retry")
		return 2
	}
	common.LogWriterMu.Lock()
	previousWriter, previousErrorWriter := gin.DefaultWriter, gin.DefaultErrorWriter
	gin.DefaultWriter, gin.DefaultErrorWriter = stderr, stderr
	common.LogWriterMu.Unlock()
	defer func() {
		common.LogWriterMu.Lock()
		gin.DefaultWriter, gin.DefaultErrorWriter = previousWriter, previousErrorWriter
		common.LogWriterMu.Unlock()
	}()
	_ = godotenv.Load(".env")
	if err := validateForkRuntimeConfig(); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if path := os.Getenv("SQLITE_PATH"); path != "" {
		common.SQLitePath = path
	}
	// Reuse the existing connection-only opener: no migration, setup user or
	// application background jobs. SQLite list opens mode=ro/query_only.
	if err := model.InitSubscriptionLinkBackfillDB(*retry); err != nil {
		fmt.Fprintln(stderr, "cannot open existing migrated fork database")
		return 1
	}
	sqlDB, err := model.DB.DB()
	if err != nil {
		fmt.Fprintln(stderr, "database connection unavailable")
		return 1
	}
	defer sqlDB.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var report any
	exitCode := 0
	if !*retry {
		report, err = model.ListUnresolvedBillingSessions(ctx, *limit)
	} else if *requestID != "" {
		err = service.RecoverBillingSession(ctx, *requestID)
		report = map[string]any{"request_id": *requestID, "recovered": err == nil}
	} else {
		var result service.BillingRecoveryResult
		result, err = service.RecoverBillingSessions(ctx, *limit)
		report = result
		if result.Failed > 0 {
			exitCode = 1
		}
	}
	if err != nil {
		fmt.Fprintln(stderr, "billing operation incomplete; unknown outcomes require evidence, not automatic refund")
		return 1
	}
	data, err := common.Marshal(report)
	if err == nil {
		_, err = fmt.Fprintln(stdout, string(data))
	}
	if err != nil {
		fmt.Fprintln(stderr, "cannot write report")
		return 1
	}
	return exitCode
}
