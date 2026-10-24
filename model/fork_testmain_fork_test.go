package model

import (
	"sync"

	"gorm.io/gorm"
)

// The upstream model TestMain migrates a fixed table list on its shared
// in-memory DB and does not know about fork tables. Upstream tests that go
// through SubscriptionOrder.Insert / CompleteSubscriptionOrder reach the fork
// hooks, so the fork table must exist there as well. Instead of editing the
// upstream test file, migrate it on that DB the first time each test reaches
// forkSubscriptionLinkEnabled.
var forkTestMigrated sync.Map // *gorm.DB -> struct{}

func init() {
	forkTestBeforeHook = func() {
		db := DB
		if db == nil {
			return
		}
		if _, done := forkTestMigrated.LoadOrStore(db, struct{}{}); done {
			return
		}
		if err := db.Session(&gorm.Session{NewDB: true}).AutoMigrate(&ForkSubscriptionOrderLink{}); err != nil {
			panic("fork test: migrate fork_subscription_order_links: " + err.Error())
		}
	}
}
