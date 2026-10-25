package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestLinkBackfillDryRunPreservesDatabase(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	path := filepath.Join(dir, "legacy.db")
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&model.SubscriptionOrder{}, &model.UserSubscription{}, &model.ForkSubscriptionOrderLink{}))
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	previousDB, previousPath := model.DB, common.SQLitePath
	previousMain, previousLog := common.MainDatabaseType(), common.LogDatabaseType()
	t.Cleanup(func() {
		model.DB, common.SQLitePath = previousDB, previousPath
		common.SetDatabaseTypes(previousMain, previousLog)
	})
	t.Setenv("SQL_DSN", "local")
	t.Setenv("SQLITE_PATH", path)
	var stdout, stderr bytes.Buffer
	require.Equal(t, 0, runLinkBackfill(nil, &stdout, &stderr), stderr.String())
	var report struct {
		Apply     bool                     `json:"apply"`
		Decisions []model.BackfillDecision `json:"decisions"`
	}
	require.NoError(t, common.Unmarshal(stdout.Bytes(), &report), stdout.String())
	assert.False(t, report.Apply)
	assert.Empty(t, report.Decisions)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestLinkBackfillRejectsInvalidArgumentsBeforeOpeningDatabase(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, args := range [][]string{{"--window", "-1"}, {"unexpected"}} {
		var stdout, stderr bytes.Buffer
		assert.Equal(t, 2, runLinkBackfill(args, &stdout, &stderr))
		assert.Empty(t, stdout.String())
		assert.NotEmpty(t, stderr.String())
	}
}
