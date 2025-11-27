package dataq

import (
	"encoding/json"
	"fmt"
	"time"
)

type QBool struct {
	Value bool
	Valid bool
}

func NewQBool(value bool) *QBool {
	return &QBool{
		Value: value,
		Valid: true,
	}
}

func InitQBool(value bool) QBool {
	return QBool{
		Value: value,
		Valid: true,
	}
}

// Set the value and force to update
func (t *QBool) Set(value bool) *QBool {
	t.Value = value
	t.Valid = true

	return t
}

func (t QBool) MarshalBinary() ([]byte, error) {
	if t.Value {
		return json.Marshal(true)
	}

	return json.Marshal(false)
}

func (t *QBool) UnmarshalBinary(data []byte) error {
	err := json.Unmarshal(data, &t.Value)
	t.Valid = (err == nil)

	return err
}

func (t QBool) MarshalJSON() ([]byte, error) {
	if t.Value {
		return json.Marshal(true)
	}

	return json.Marshal(false)
}

func (t *QBool) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &t.Value)
	t.Valid = (err == nil)

	return err
}

func (t QBool) String() string {
	return fmt.Sprintf("%t", t.Value)
}

type QInt struct {
	Value int
	Valid bool
}

func NewQInt(value int) *QInt {
	return &QInt{
		Value: value,
		Valid: true,
	}
}

func InitQInt(value int) QInt {
	return QInt{
		Value: value,
		Valid: true,
	}
}

// Set the value and force to update
func (t *QInt) Set(value int) *QInt {
	t.Value = value
	t.Valid = true

	return t
}

func (t QInt) MarshalBinary() ([]byte, error) {
	return json.Marshal(t.Value)
}

func (t *QInt) UnmarshalBinary(data []byte) error {
	err := json.Unmarshal(data, &t.Value)
	t.Valid = (err == nil)

	return err
}

func (t QInt) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Value)
}

func (t *QInt) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &t.Value)
	t.Valid = (err == nil)

	return err
}

func (t QInt) String() string {
	return fmt.Sprintf("%d", t.Value)
}

type QFloat64 struct {
	Value float64
	Valid bool
}

func NewQFloat64(value float64) *QFloat64 {
	return &QFloat64{
		Value: value,
		Valid: true,
	}
}

func InitQFloat64(value float64) QFloat64 {
	return QFloat64{
		Value: value,
		Valid: true,
	}
}

// Set the value and force to update
func (t *QFloat64) Set(value float64) *QFloat64 {
	t.Value = value

	return t
}

func (t QFloat64) MarshalBinary() ([]byte, error) {
	return json.Marshal(t.Value)
}

func (t *QFloat64) UnmarshalBinary(data []byte) error {
	err := json.Unmarshal(data, &t.Value)
	t.Valid = (err == nil)

	return err
}

func (t QFloat64) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Value)
}

func (t *QFloat64) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &t.Value)
	t.Valid = (err == nil)

	return err
}

func (t QFloat64) String() string {
	return fmt.Sprintf("%f", t.Value)
}

type QString struct {
	Value string
	Valid bool
}

func NewQString(value string) *QString {
	return &QString{
		Value: value,
		Valid: true,
	}
}

func InitQString(value string) QString {
	return QString{
		Value: value,
		Valid: true,
	}
}

// Set the value and force to update
func (t *QString) Set(value string) *QString {
	t.Value = value
	t.Valid = true

	return t
}

func (t QString) MarshalBinary() ([]byte, error) {
	return json.Marshal(t.Value)
}

func (t *QString) UnmarshalBinary(data []byte) error {
	err := json.Unmarshal(data, &t.Value)
	t.Valid = (err == nil)

	return err
}

func (t QString) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Value)
}

func (t *QString) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &t.Value)
	t.Valid = (err == nil)

	return err
}

func (t QString) String() string {
	return t.Value
}

type QStrings struct {
	Value []string
	Valid bool
}

func NewQStrings(value []string) *QStrings {
	return &QStrings{
		Value: value,
		Valid: true,
	}
}

func InitQStrings(value []string) QStrings {
	return QStrings{
		Value: value,
		Valid: true,
	}
}

// Set the value and force to update
func (t *QStrings) Set(value []string) *QStrings {
	t.Value = value
	t.Valid = true

	return t
}

func (t *QStrings) RemoveIndex(idx int) {
	t.Value = append(t.Value[:idx], t.Value[idx+1:]...)
}

func (t *QStrings) RemoveValue(value string) {
	for idx, v := range t.Value {
		if v == value {
			t.RemoveIndex(idx)
		}
	}
}

func (t QStrings) MarshalBinary() ([]byte, error) {
	return json.Marshal(t.Value)
}

func (t *QStrings) UnmarshalBinary(data []byte) error {
	err := json.Unmarshal(data, &t.Value)
	t.Valid = (err == nil)

	return err
}

func (t QStrings) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Value)
}

func (t *QStrings) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &t.Value)
	t.Valid = (err == nil)

	return err
}

func (t QStrings) String() string {
	return fmt.Sprintf("%v", t.Value)
}

type QTime struct {
	Value time.Time
	Valid bool
}

func NewQTime(value time.Time) *QTime {
	return &QTime{
		Value: value,
		Valid: true,
	}
}

func InitQTime(value time.Time) QTime {
	return QTime{
		Value: value,
		Valid: true,
	}
}

// Set the value and force to update
func (t *QTime) Set(value time.Time) *QTime {
	t.Value = value
	t.Valid = true

	return t
}

func (t QTime) MarshalBinary() ([]byte, error) {
	return json.Marshal(t.Value)
}

func (t *QTime) UnmarshalBinary(data []byte) error {
	if len(data) <= 2 {
		t.Valid = true
		t.Value = time.Time{}
		return nil
	}

	err := json.Unmarshal(data, &t.Value)
	t.Valid = (err == nil)

	return err
}

func (t QTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Value)
}

func (t *QTime) UnmarshalJSON(data []byte) error {
	if len(data) <= 2 {
		t.Valid = true
		t.Value = time.Time{}
		return nil
	}
	err := json.Unmarshal(data, &t.Value)
	t.Valid = (err == nil)

	return err
}

func (t QTime) String() string {
	return t.Value.Format(ConfigParseDateTimeFormat)
}
