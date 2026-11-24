package evaluation

import "github.com/google/uuid"

// Fixture lifecycle headers carry the receipt and the server's ownership result.
const (
	ReceiptHeader      = "X-Quarry-Evaluation-Receipt"
	StateHeader        = "X-Quarry-Evaluation-State"
	IncarnationHeader  = "X-Document-Incarnation"
	DatabaseTimeHeader = "X-Quarry-Evaluation-Database-Time"
	ReconcileHeader    = "X-Quarry-Evaluation-Reconcile"
)

// ValidReceipt reports whether the receipt is a canonical random UUID.
func ValidReceipt(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id.String() == value && id.Version() == 4 && id.Variant() == uuid.RFC4122
}

// ValidIncarnation reports whether the revision is a canonical nonzero UUID.
func ValidIncarnation(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id.String() == value && id != uuid.Nil
}
