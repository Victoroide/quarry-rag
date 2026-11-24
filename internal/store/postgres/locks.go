package postgres

// Advisory lock keys reserved by Quarry. Keep existing values stable across replicas.
const (
	migrationLockID     int64 = 0x7175617272790001
	ingestAdmissionLock int64 = 0x7175617272790002
	vectorIndexLockID   int64 = 0x7175617272790003
)
