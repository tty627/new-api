package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type BillingRecoveryResult struct {
	Scanned   int `json:"scanned"`
	Recovered int `json:"recovered"`
	Failed    int `json:"failed"`
	Unknown   int `json:"unknown"`
}

func RecoverBillingSessions(ctx context.Context, limit int) (BillingRecoveryResult, error) {
	result := BillingRecoveryResult{}
	rows, err := model.ListRecoverableBillingSessions(ctx, limit)
	if err != nil {
		return result, err
	}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		result.Scanned++
		if row.NextRetryAt > common.GetTimestamp() {
			model.RefreshBillingQuotaCaches(ctx, &row)
			continue
		}
		if row.Status == model.BillingReserved || row.Status == model.BillingTaskActive {
			model.RefreshBillingQuotaCaches(ctx, &row)
			result.Unknown++
			continue
		}
		if err := model.ApplyBillingIntent(ctx, row.RequestID); err != nil {
			result.Failed++
			continue
		}
		result.Recovered++
	}
	return result, nil
}

func RecoverBillingSession(ctx context.Context, requestID string) error {
	return model.ApplyBillingIntent(ctx, requestID)
}

func billingOperationContext() (context.Context, context.CancelFunc) {
	// Client disconnect must not cancel a known settlement/refund intent write.
	return context.WithTimeout(context.Background(), 15*time.Second)
}

func durableBillingAPIError(err error) *types.NewAPIError {
	switch {
	case errors.Is(err, model.ErrBillingWalletInsufficient), errors.Is(err, model.ErrSubscriptionQuotaInsufficient), errors.Is(err, model.ErrNoActiveSubscription):
		return types.NewErrorWithStatusCode(err, types.ErrorCodeInsufficientUserQuota, http.StatusForbidden,
			types.ErrOptionWithSkipRetry(), types.ErrOptionWithNoRecordErrorLog())
	case errors.Is(err, model.ErrBillingTokenInsufficient):
		return types.NewErrorWithStatusCode(err, types.ErrorCodePreConsumeTokenQuotaFailed, http.StatusForbidden,
			types.ErrOptionWithSkipRetry(), types.ErrOptionWithNoRecordErrorLog())
	default:
		return types.NewError(err, types.ErrorCodeUpdateDataError, types.ErrOptionWithSkipRetry())
	}
}

func (s *BillingSession) syncDurableBilling(row *model.ForkBillingSession) {
	s.preConsumedQuota = common.QuotaFromFloat(float64(row.ReservedQuota))
	s.tokenConsumed = s.preConsumedQuota
	if row.Playground {
		s.tokenConsumed = 0
	}
	if funding, ok := s.funding.(*WalletFunding); ok {
		funding.consumed = s.preConsumedQuota
	}
	if funding, ok := s.funding.(*SubscriptionFunding); ok {
		funding.subscriptionId, funding.preConsumed = row.SubscriptionID, row.ReservedQuota
		funding.AmountTotal, funding.AmountUsedAfter = row.SubscriptionAmountTotal, row.SubscriptionUsedAfter
		if plan, err := model.GetSubscriptionPlanInfoByUserSubscriptionId(row.SubscriptionID); err == nil && plan != nil {
			funding.PlanId, funding.PlanTitle = plan.PlanId, plan.PlanTitle
		}
	}
	s.extraReserved = 0
	s.syncRelayInfo()
}

func (s *BillingSession) preConsumeDurable(c *gin.Context, quota int) *types.NewAPIError {
	if s.shouldTrust(c) {
		s.trusted, quota = true, 0
	}
	ctx, cancel := billingOperationContext()
	defer cancel()
	row, created, err := model.ReserveBillingSession(ctx, model.BillingReservation{
		RequestID: s.durableRequestID, UserID: s.relayInfo.UserId, TokenID: s.relayInfo.TokenId,
		Playground: s.relayInfo.IsPlayground, FundingSource: s.funding.Source(),
		ModelName: s.relayInfo.GetBillingModelName(), Quota: quota})
	if err != nil {
		return durableBillingAPIError(err)
	}
	if !created {
		return durableBillingAPIError(model.ErrBillingOutcomeUnknown)
	}
	s.syncDurableBilling(row)
	return nil
}

func (s *BillingSession) settleDurable(actualQuota int) error {
	// A known completed upstream result must never become a refund, even when
	// the database cannot yet persist its settlement intention.
	s.fundingSettled = true
	ctx, cancel := billingOperationContext()
	defer cancel()
	if err := model.PersistBillingIntent(ctx, s.durableRequestID, model.BillingSettlePending, actualQuota); err != nil {
		return err
	}
	// A committed settle intention also prevents a subsequent error handler
	// from converting a database outage into an incorrect refund.
	s.fundingSettled = true
	if err := model.ApplyBillingIntent(ctx, s.durableRequestID); err != nil {
		return err
	}
	if !s.settled && s.funding.Source() == BillingSourceSubscription {
		s.relayInfo.SubscriptionPostDelta += int64(actualQuota) - int64(s.preConsumedQuota)
	}
	s.settled = true
	return nil
}

func (s *BillingSession) reserveRealtimeQuota(delta int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if delta < 0 || int64(s.preConsumedQuota)+int64(delta) > common.MaxQuota {
		return model.ErrBillingIntentConflict
	}
	ctx, cancel := billingOperationContext()
	defer cancel()
	row, err := model.ResizeBillingSession(ctx, s.durableRequestID, s.preConsumedQuota+delta, true)
	if err != nil {
		return err
	}
	s.syncDurableBilling(row)
	return nil
}

// BillingFailureOutcomeUnknown is deliberately conservative for transport and
// post-response decoding failures; receiving no usable answer proves no refund.
func BillingFailureOutcomeUnknown(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var network net.Error
	if errors.As(err, &network) {
		return true
	}
	var apiErr *types.NewAPIError
	if errors.As(err, &apiErr) {
		switch apiErr.GetErrorCode() {
		case types.ErrorCodeDoRequestFailed, types.ErrorCodeReadResponseBodyFailed, types.ErrorCodeBadResponse,
			types.ErrorCodeBadResponseBody, types.ErrorCodeEmptyResponse,
			types.ErrorCodeChannelResponseTimeExceeded, types.ErrorCodeAwsInvokeError:
			return true
		}
	}
	return false
}

func (s *BillingSession) refundDurable(c *gin.Context) {
	ctx, cancel := billingOperationContext()
	defer cancel()
	if BillingFailureOutcomeUnknown(s.relayInfo.LastError) ||
		(c != nil && c.Request != nil && c.Request.Context().Err() != nil) {
		if err := model.MarkBillingOutcomeUnknown(ctx, s.durableRequestID); err != nil {
			common.SysError("durable billing could not record unknown upstream outcome")
		}
		return
	}
	if err := model.PersistBillingIntent(ctx, s.durableRequestID, model.BillingRefundPending, 0); err != nil {
		common.SysError("durable billing refund intention was not persisted")
		return
	}
	if err := model.ApplyBillingIntent(ctx, s.durableRequestID); err != nil {
		common.SysError("durable billing refund remains pending for recovery")
		return
	}
	s.refunded = true
}

// AcceptTaskBilling keeps an asynchronous reservation open until polling has a
// known terminal result. Acceptance itself is a recoverable durable intention.
func PersistAcceptedTask(c *gin.Context, info *relaycommon.RelayInfo, task *model.Task, quota int, omitColumns ...string) error {
	if info == nil || task == nil {
		return model.ErrBillingIntentConflict
	}
	s, ok := info.Billing.(*BillingSession)
	if !ok || s.durableRequestID == "" {
		return task.InsertWithContext(c.Request.Context(), omitColumns...)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fundingSettled = true
	ctx, cancel := billingOperationContext()
	defer cancel()
	return model.CreateAcceptedBillingTask(ctx, s.durableRequestID, task, quota, info.QuotaClamp, omitColumns...)
}

func AcceptTaskBilling(c *gin.Context, info *relaycommon.RelayInfo, task *model.Task, quota int) error {
	if info == nil || task == nil {
		return model.ErrBillingIntentConflict
	}
	if info.Billing == nil && quota == 0 {
		return nil
	}
	s, ok := info.Billing.(*BillingSession)
	if !ok || s.durableRequestID == "" {
		return SettleBilling(c, info, quota)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fundingSettled = true // upstream accepted: a local DB error cannot refund it
	ctx, cancel := billingOperationContext()
	defer cancel()
	if err := model.AttachBillingTask(ctx, s.durableRequestID, task.ID); err != nil {
		return err
	}
	intent := model.BillingTaskAcceptPending
	if task.Status == model.TaskStatusSuccess || task.Status == model.TaskStatusFailure {
		intent = model.BillingSettlePending
	}
	row, err := model.GetBillingSession(ctx, s.durableRequestID)
	if err != nil {
		return err
	}
	if row.Status == model.BillingReserved {
		if err := model.PersistBillingIntent(ctx, s.durableRequestID, intent, quota); err != nil {
			return err
		}
		row, err = model.GetBillingSession(ctx, s.durableRequestID)
		if err != nil {
			return err
		}
	}
	if row.Status == model.BillingTaskAcceptPending || row.Status == model.BillingSettlePending || row.Status == model.BillingRefundPending {
		if err := model.ApplyBillingIntent(ctx, s.durableRequestID); err != nil {
			return err
		}
		row, err = model.GetBillingSession(ctx, s.durableRequestID)
		if err != nil {
			return err
		}
	}
	s.syncDurableBilling(row)
	s.settled = intent == model.BillingSettlePending
	task.Quota = quota
	return nil
}

func chargeDurableViolationFee(info *relaycommon.RelayInfo, quota int) (bool, error) {
	if info.RequestId == "" {
		info.RequestId = common.NewRequestId()
	}
	id := fmt.Sprintf("%x", sha256.Sum256([]byte(info.RequestId+":violation")))
	source := info.BillingSource
	if source == "" {
		source = BillingSourceWallet
	}
	reserve := 0
	if source == BillingSourceSubscription {
		reserve = quota
	}
	ctx, cancel := billingOperationContext()
	defer cancel()
	_, created, err := model.ReserveBillingSession(ctx, model.BillingReservation{RequestID: id, UserID: info.UserId,
		TokenID: info.TokenId, Playground: info.IsPlayground, FundingSource: source, ModelName: info.GetBillingModelName(), Quota: reserve})
	if err != nil {
		return false, err
	}
	if err := model.PersistBillingIntent(ctx, id, model.BillingSettlePending, quota); err != nil {
		return false, err
	}
	applied, err := model.ApplyBillingIntentOnce(ctx, id)
	return created && applied, err
}

func settleDurableTask(ctx context.Context, task *model.Task, actual int, refund bool) (bool, error) {
	if task == nil || task.ID <= 0 {
		return false, model.ErrBillingOutcomeUnknown
	}
	requestID, err := billingTaskRequestID(ctx, task)
	if err != nil {
		return false, err
	}
	row, err := model.GetBillingSession(ctx, requestID)
	if err != nil {
		return false, err
	}
	if row.TaskID != task.ID || row.UserID != task.UserId || row.TokenID != task.PrivateData.TokenId {
		return false, model.ErrBillingIntentConflict
	}
	if row.Status == model.BillingSettled || row.Status == model.BillingRefunded {
		if row.ActualQuota != nil {
			task.Quota = int(*row.ActualQuota)
		}
		return false, nil
	}
	intent := model.BillingSettlePending
	if refund {
		intent = model.BillingRefundPending
	}
	if err := model.PersistBillingIntent(ctx, requestID, intent, actual); err != nil {
		return false, err
	}
	applied, err := model.ApplyBillingIntentOnce(ctx, requestID)
	if err != nil {
		return false, err
	}
	task.Quota = actual
	return applied, nil
}

func billingTaskRequestID(ctx context.Context, task *model.Task) (string, error) {
	if task.PrivateData.Execution != nil && task.PrivateData.Execution.RequestID != "" {
		id := task.PrivateData.Execution.RequestID
		row, err := model.GetBillingSession(ctx, id)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				if task.Quota == 0 && task.PrivateData.BillingSource == "" && task.PrivateData.SubscriptionId == 0 {
					return "", err
				}
				return model.ImportLegacyWalletTask(ctx, task.ID)
			}
			return "", err
		}
		if row.TaskID != task.ID {
			return "", model.ErrBillingIntentConflict
		}
		return id, nil
	}
	return model.ImportLegacyWalletTask(ctx, task.ID)
}

func commitTerminalTaskBilling(ctx context.Context, adaptor TaskPollingAdaptor, task *model.Task, result *relaycommon.TaskInfo, from model.TaskStatus, known bool) (bool, error) {
	intent, actual := model.BillingSettlePending, task.Quota
	var clamp *common.QuotaClamp
	if !known {
		intent = ""
	} else if bc := task.PrivateData.BillingContext; bc != nil && bc.TieredSnapshot != nil {
		if task.Status == model.TaskStatusFailure {
			intent, actual = model.BillingRefundPending, 0
		} else {
			priced, facts, err := EvaluateTaskCompletionUsage(bc.TieredSnapshot, result.UsageFacts)
			if err != nil {
				intent = ""
			} else {
				actual = priced.ActualQuotaAfterGroup
				clamp = priced.Clamp
				bc.TieredSnapshot.UsageFacts = facts
				bc.TieredSnapshot.EstimatedTier = priced.MatchedTier
			}
		}
	} else if bc := task.PrivateData.BillingContext; bc != nil && bc.PerCallBilling {
		if task.Status == model.TaskStatusFailure {
			intent, actual = model.BillingRefundPending, 0
		}
	} else if adjusted := adaptor.AdjustBillingOnComplete(task, result); adjusted > 0 {
		actual = adjusted
	} else {
		tokens := result.TotalTokens
		if tokens == 0 && result.CompletionTokens > 0 {
			tokens = result.CompletionTokens
		}
		if quota, _, tokenClamp, ok := calculateTaskQuotaByTokens(task, tokens); ok {
			actual = quota
			clamp = tokenClamp
		} else if task.Status == model.TaskStatusFailure {
			intent, actual = model.BillingRefundPending, 0
		}
	}
	id, err := billingTaskRequestID(ctx, task)
	if err != nil {
		// Historical subscription cycles cannot be invented from a task snapshot.
		hasRequest := task.PrivateData.Execution != nil && task.PrivateData.Execution.RequestID != ""
		historicalUnknown := errors.Is(err, model.ErrBillingOutcomeUnknown) || (task.PrivateData.BillingSource == BillingSourceSubscription && !hasRequest)
		freeTask := errors.Is(err, gorm.ErrRecordNotFound) && task.Quota == 0 && actual == 0 && task.PrivateData.BillingSource == "" && task.PrivateData.SubscriptionId == 0
		if historicalUnknown || freeTask {
			return task.UpdateWithStatus(from)
		}
		return false, err
	}
	return model.CommitTaskBillingResult(ctx, id, task, from, intent, actual, clamp)
}
