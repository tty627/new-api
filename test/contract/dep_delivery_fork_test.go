package contract_test

import (
	"errors"
	"net/url"
	"os"
	"sync"
	"testing"

	"github.com/Calcium-Ion/go-epay/epay"
	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// This fixture is produced by the Python bridge's success_params function,
// rather than by this package's Go signing helper. Ignore the first response
// to model a lost acknowledgement, then replay the exact bridge payload.
func TestDEP_BridgeGeneratedNotifyAfterLostResponse(t *testing.T) {
	data, err := os.ReadFile("testdata/bridge_delivery.json")
	require.NoError(t, err)
	var fixture struct {
		PID   string `json:"pid"`
		Key   string `json:"key"`
		Cases []struct {
			BusinessKind string            `json:"business_kind"`
			Params       map[string]string `json:"params"`
		} `json:"cases"`
	}
	require.NoError(t, common.Unmarshal(data, &fixture))
	require.Len(t, fixture.Cases, 2)
	forEachDB(t, func(t *testing.T, db *gorm.DB) {
		configureEpay(t)
		operation_setting.EpayId, operation_setting.EpayKey = fixture.PID, fixture.Key
		for _, sample := range fixture.Cases {
			t.Run(sample.BusinessKind, func(t *testing.T) {
				user := createUser(t, db, 0)
				tradeNo := sample.Params["out_trade_no"]
				require.Equal(t, "7.30", sample.Params["money"])
				path := "/api/user/epay/notify"
				if sample.BusinessKind == "subscription" {
					plan := createPlan(t, db, 7.30)
					createPendingSubscriptionOrder(t, db, user.Id, plan, tradeNo)
					path = "/api/subscription/epay/notify"
				} else {
					require.Equal(t, "topup", sample.BusinessKind)
					createPendingTopUp(t, db, user.Id, tradeNo, 10, 7.30)
				}
				form := url.Values{}
				for key, value := range sample.Params {
					form.Set(key, value)
				}
				router := callbackRouter()
				postForm(router, path, form) // The bridge did not receive this response.
				require.Equal(t, "success", postForm(router, path, form).Body.String())
				if sample.BusinessKind == "subscription" {
					assert.EqualValues(t, 1, countSubscriptions(t, db, user.Id))
					assert.Equal(t, common.TopUpStatusSuccess, orderStatus(t, db, &model.SubscriptionOrder{}, tradeNo))
					status, err := model.GetSubscriptionLinkStatus(tradeNo)
					require.NoError(t, err)
					assert.Equal(t, model.LinkStatusLinked, status.Status)
				} else {
					assert.Equal(t, int(10*common.QuotaPerUnit), userQuota(t, db, user.Id))
					assert.Equal(t, common.TopUpStatusSuccess, orderStatus(t, db, &model.TopUp{}, tradeNo))
				}
			})
		}
	})
}

// DEP-01: a top-up notify whose settlement transaction fails must not answer
// "success", so the bridge keeps retrying.
func TestDEP01_TopupNotifyFailsWhenSettlementFails(t *testing.T) {
	forEachDB(t, func(t *testing.T, db *gorm.DB) {
		configureEpay(t)
		user := createUser(t, db, 0)
		createPendingTopUp(t, db, user.Id, "DEP01TOPUP", 10, 7.30)

		const cb = "contract:fail_topup_update"
		require.NoError(t, db.Callback().Update().Before("gorm:update").Register(cb, func(tx *gorm.DB) {
			if tx.Statement.Table == "top_ups" {
				_ = tx.AddError(errors.New("injected settlement failure"))
			}
		}))
		router := callbackRouter()
		form := signedCallback(t, "DEP01TOPUP", "7.30", epay.StatusTradeSuccess)

		rec := postForm(router, "/api/user/epay/notify", form)
		require.NoError(t, db.Callback().Update().Remove(cb))

		assert.NotEqual(t, "success", rec.Body.String())
		assert.Equal(t, common.TopUpStatusPending, orderStatus(t, db, &model.TopUp{}, "DEP01TOPUP"))
		assert.Equal(t, 0, userQuota(t, db, user.Id))

		// Control: once the fault is gone, the bridge's retry settles exactly once.
		rec = postForm(router, "/api/user/epay/notify", form)
		assert.Equal(t, "success", rec.Body.String())
		assert.Equal(t, common.TopUpStatusSuccess, orderStatus(t, db, &model.TopUp{}, "DEP01TOPUP"))
		assert.Equal(t, int(10*common.QuotaPerUnit), userQuota(t, db, user.Id))
	})
}

// DEP-01: a badly signed subscription notify answers "fail" and grants nothing.
func TestDEP01_SubscriptionNotifyRejectsBadSignature(t *testing.T) {
	forEachDB(t, func(t *testing.T, db *gorm.DB) {
		configureEpay(t)
		user := createUser(t, db, 0)
		plan := createPlan(t, db, 19.90)
		createPendingSubscriptionOrder(t, db, user.Id, plan, "DEP01SUBSIG")

		form := signedCallback(t, "DEP01SUBSIG", "19.90", epay.StatusTradeSuccess)
		form.Set("sign", "0000000000000000000000000000000")
		rec := postForm(callbackRouter(), "/api/subscription/epay/notify", form)

		assert.Equal(t, "fail", rec.Body.String())
		assert.Equal(t, common.TopUpStatusPending, orderStatus(t, db, &model.SubscriptionOrder{}, "DEP01SUBSIG"))
		assert.Zero(t, countSubscriptions(t, db, user.Id))
	})
}

// DEP-02: a repeated success notify is acknowledged and credits quota once.
func TestDEP02_DuplicateTopupNotifyIsIdempotent(t *testing.T) {
	forEachDB(t, func(t *testing.T, db *gorm.DB) {
		configureEpay(t)
		user := createUser(t, db, 0)
		createPendingTopUp(t, db, user.Id, "DEP02DUP", 3, 2.20)
		router := callbackRouter()
		form := signedCallback(t, "DEP02DUP", "2.20", epay.StatusTradeSuccess)

		for i := 0; i < 2; i++ {
			assert.Equal(t, "success", postForm(router, "/api/user/epay/notify", form).Body.String(), "attempt %d", i+1)
		}
		assert.Equal(t, int(3*common.QuotaPerUnit), userQuota(t, db, user.Id))
	})
}

// DEP-02: concurrent duplicate subscription notifies create one subscription.
// Correctness must come from the database (row locks / immediate SQLite
// transactions), not from the in-process order lock alone.
func TestDEP02_ConcurrentSubscriptionNotifyCreatesOneSubscription(t *testing.T) {
	forEachDB(t, func(t *testing.T, db *gorm.DB) {
		configureEpay(t)
		user := createUser(t, db, 0)
		plan := createPlan(t, db, 29.00)
		createPendingSubscriptionOrder(t, db, user.Id, plan, "DEP02CONC")
		router := callbackRouter()
		form := signedCallback(t, "DEP02CONC", "29.00", epay.StatusTradeSuccess)

		const workers = 8
		bodies := make([]string, workers)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				bodies[i] = postForm(router, "/api/subscription/epay/notify", form).Body.String()
			}()
		}
		close(start)
		wg.Wait()

		for i, body := range bodies {
			assert.Equal(t, "success", body, "worker %d", i)
		}
		assert.EqualValues(t, 1, countSubscriptions(t, db, user.Id))
		assert.Equal(t, common.TopUpStatusSuccess, orderStatus(t, db, &model.SubscriptionOrder{}, "DEP02CONC"))
	})
}
