package contract_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/QuantumNous/new-api/controller"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// moneyInputs are plan prices chosen around float64 rounding edges.
// Values must fit SubscriptionPlan.PriceAmount decimal(10,6) and be >= 0.01.
var moneyInputs = []float64{0.01, 0.015, 0.1, 1, 1.005, 2.675, 9.995, 10.005, 19.9, 123.456, 9999.999999}

type moneyGolden struct {
	Input float64 `json:"input"`
	Money string  `json:"money"`
}

// DEP-03: the `money` New API sends to Epay is FormatFloat(price,'f',2,64) of
// the price as read back from the database. The bridge reconciler reproduces
// this rule using testdata/money_golden.json.
func TestDEP03_MoneySerialization(t *testing.T) {
	golden := readMoneyGolden(t)
	forEachDB(t, func(t *testing.T, db *gorm.DB) {
		configureEpay(t)
		user := createUser(t, db, 0)
		router := newPayRouter(user.Id)
		for _, input := range moneyInputs {
			plan := createPlan(t, db, input)
			data := requestSubscriptionPay(t, router, plan.Id)
			assert.Equal(t, golden[input], data["money"], "price %v", input)
		}
	})
}

// newPayRouter serves the subscription Epay request handler as userID.
func newPayRouter(userID int) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/api/subscription/epay/pay", func(c *gin.Context) {
		c.Set("id", userID)
		controller.SubscriptionRequestEpay(c)
	})
	return router
}

// requestSubscriptionPay creates a pending order and returns the signed form
// New API hands to the browser for submission to Epay.
func requestSubscriptionPay(t *testing.T, router http.Handler, planID int) map[string]string {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"plan_id": planID, "payment_method": "alipay"})
	req := httptest.NewRequest(http.MethodPost, "/api/subscription/epay/pay", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	var resp struct {
		Message string            `json:"message"`
		Data    map[string]string `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp), rec.Body.String())
	require.Equal(t, "success", resp.Message, rec.Body.String())
	return resp.Data
}

// readMoneyGolden loads the shared golden file and checks it matches Go's
// formatting, regenerating it when CONTRACT_UPDATE_GOLDEN=1.
func readMoneyGolden(t *testing.T) map[float64]string {
	t.Helper()
	path := filepath.Join("testdata", "money_golden.json")
	want := make([]moneyGolden, 0, len(moneyInputs))
	for _, in := range moneyInputs {
		want = append(want, moneyGolden{Input: in, Money: strconv.FormatFloat(in, 'f', 2, 64)})
	}
	if os.Getenv("CONTRACT_UPDATE_GOLDEN") == "1" {
		data, err := json.MarshalIndent(want, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(path, append(data, '\n'), 0o644))
	}
	data, err := os.ReadFile(path)
	require.NoError(t, err, "run with CONTRACT_UPDATE_GOLDEN=1 to create %s", path)
	var got []moneyGolden
	require.NoError(t, json.Unmarshal(data, &got))
	require.Equal(t, want, got, "golden file is stale; regenerate with CONTRACT_UPDATE_GOLDEN=1")
	out := make(map[float64]string, len(got))
	for _, g := range got {
		out[g.Input] = g.Money
	}
	return out
}
