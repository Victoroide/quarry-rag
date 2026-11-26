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
const DeletionActivationLockID int64 = 0x7175617272790004
