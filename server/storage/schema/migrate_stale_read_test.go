package schema

import (
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/api/v3/version"
	betesting "go.etcd.io/etcd/server/v3/storage/backend/testing"
)

// The storage-version migration from v3.5 to v3.7 puts meta/storageVersion three times in
// one batch-lock session ("", "3.6.0", "3.7.0"); after a commit the meta bucket's read-buffer
// entry is present but empty, so the write-back keeps all three and ReadTx reads the first.
func TestMigrateStaleRead(t *testing.T) {
	lg := zaptest.NewLogger(t)
	be, _ := betesting.NewTmpBackend(t, time.Hour, 10000)
	defer betesting.Close(t, be)

	tx := be.BatchTx()
	tx.LockOutsideApply()
	UnsafeCreateMetaBucket(tx)
	UnsafeUpdateConsistentIndex(tx, 1, 1)
	tx.Unlock()
	be.ForceCommit()

	tx.LockOutsideApply()
	if err := UnsafeMigrate(lg, tx, nil, version.V3_7); err != nil {
		t.Fatal(err)
	}
	tx.Unlock()

	rv, rerr := DetectSchemaVersion(lg, be.ReadTx())
	tx.LockOutsideApply()
	bv := UnsafeReadStorageVersion(tx)
	tx.Unlock()
	if rerr != nil || bv == nil || rv.String() != bv.String() {
		t.Fatalf("ReadTx: %v (err %v); BatchTx: %v", rv, rerr, bv)
	}
}
