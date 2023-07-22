package dataq

import (
	"encoding/json"
	"time"
)

type QBool struct {
	Value bool
	Valid bool
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

type QFloat64 struct {
	Value float64
	Valid bool
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

type QString struct {
	Value string
	Valid bool
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

type QTime struct {
	Value time.Time
	Valid bool
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
