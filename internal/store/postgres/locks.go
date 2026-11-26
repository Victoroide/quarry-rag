package postgres

// Advisory lock keys reserved by Quarry. Keep existing values stable across replicas.
const (
	migrationLockID     int64 = 0x7175617272790001
	ingestAdmissionLock int64 = 0x7175617272790002
	vectorIndexLockID   int64 = 0x7175617272790003
)

// DeletionActivationLockID orders ID-only cleanup against activation. Cleanup
// holds it shared for the whole external sequence. The writer of the activation
// marker takes it exclusively, so activation commits only after a healthy
// cleanup has returned. The durable reservation covers a session that is lost.
// It is always the outermost lock: no path takes it while holding another
// advisory lock or the marker row, so activation, which locks the marker row
// under it, cannot deadlock with admission, migrations or maintenance.
const DeletionActivationLockID int64 = 0x7175617272790004

// documentIntentLockKey is the per-document advisory key that orders a
// deletion intent with download signing. It hashes the canonical UUID with its
// own seed, independent of the fixture reservation key (seed 487011). A hash
// collision between two documents only delays one of them.
const documentIntentLockKey = "hashtextextended($1::uuid::text, 487012)"

// scopedFilePrincipalLockClass separates signing and grant import locks from
// the bigint document intent key. Both SQL callers use this class in migration 034.
const scopedFilePrincipalLockClass int32 = 0x71726673
