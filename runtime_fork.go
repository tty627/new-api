package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/service"
)

// Durable billing commits money and token balances in the primary database.
// Legacy asynchronous balance queues would violate that transaction boundary.
func validateForkRuntimeConfig() error {
	if os.Getenv("BATCH_UPDATE_ENABLED") == "true" {
		return fmt.Errorf("durable billing requires BATCH_UPDATE_ENABLED=false; drain legacy queued writes before upgrading")
	}
	host := os.Getenv("BIND_HOST")
	if host != "" && host != "localhost" && net.ParseIP(host) == nil {
		return fmt.Errorf("BIND_HOST must be an IP address or localhost")
	}
	return nil
}

// Every instance may run this loop: database transactions arbitrate completion.
// Reserved sessions with an unknown upstream result are never auto-refunded.
func startForkBillingRecovery() func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			passCtx, stopPass := context.WithTimeout(ctx, 30*time.Second)
			result, err := service.RecoverBillingSessions(passCtx, 200)
			stopPass()
			if ctx.Err() != nil {
				return
			}
			if err != nil || result.Failed > 0 {
				common.SysError("billing recovery has pending failures; inspect billing-recovery list")
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() { cancel(); <-done }
}
