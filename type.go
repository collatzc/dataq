package dataq

import (
	"encoding/json"
	"time"
)

type QBool struct {
	Value bool
	Valid bool
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
