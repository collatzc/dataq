package dataq

import (
	"fmt"
)

// QResult ...
type QResult struct {
	LastInsertId int64
	AffectedRows int64
	Warning      uint
	Error        error
}

func (re *QResult) String() string {
	return fmt.Sprintf("Query Result {\nAffectedRows: %d\nLastInsertId: %d\nError: %v\n}", re.AffectedRows, re.LastInsertId, re.Error)
}
