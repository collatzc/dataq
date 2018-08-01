package dataq

import (
	"fmt"
)

// QResult encapsulates the result of the SQL query
type QResult struct {
	AffectedRows int64
	LastInsertId int64
	ReturnedRows int64
	Warning      uint
	Error        error
}

func (re *QResult) String() string {
	return fmt.Sprintf("QResult {\n\tAffectedRows: %d\n\tLastInsertId: %d\n\tReturnedRows: %d\n\tError: %v\n}", re.AffectedRows, re.LastInsertId, re.ReturnedRows, re.Error)
}
