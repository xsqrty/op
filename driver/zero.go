package driver

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"time"
)

type ZeroFloat64 float64
type ZeroString string
type ZeroInt64 int64
type ZeroTime time.Time
type ZeroBool bool

var (
	timeNull  = []byte("null")
	timeEmpty = []byte(`""`)
	timeZero  = []byte("0")
)

// IsZero reports whether t represents zero time,
// this is a wrapper around the method `time.Time.IsZero()`
func (zt ZeroTime) IsZero() bool {
	return time.Time(zt).IsZero()
}

// MarshalJSON implements the [encoding/json.Marshaler] interface.
func (zt ZeroTime) MarshalJSON() ([]byte, error) {
	return time.Time(zt).MarshalJSON()
}

// UnmarshalJSON implements the [encoding/json.Unmarshaler] interface.
func (zt *ZeroTime) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, timeNull) || bytes.Equal(data, timeEmpty) || bytes.Equal(data, timeZero) {
		*zt = ZeroTime(time.Time{})
		return nil
	}

	t := time.Time(*zt)
	err := (&t).UnmarshalJSON(data)
	if err != nil {
		return err
	}

	*zt = ZeroTime(t)
	return nil
}

// String returns the time formatted using the format string
// "2006-01-02 15:04:05.999999999 -0700 MST"
func (zt ZeroTime) String() string {
	return time.Time(zt).String()
}

// Value implements the [driver.Valuer] interface.
func (zt ZeroTime) Value() (driver.Value, error) {
	if zt.IsZero() {
		return time.Time{}, nil
	}

	return time.Time(zt), nil
}

// Scan implements the [Scanner] interface.
func (zt *ZeroTime) Scan(value any) error {
	if value == nil {
		*zt = ZeroTime(time.Time{})
		return nil
	}

	switch v := value.(type) {
	case time.Time:
		*zt = ZeroTime(v)
		return nil
	case []byte:
		parsed, err := time.Parse(time.RFC3339, string(v))
		if err != nil {
			return err
		}

		*zt = ZeroTime(parsed)
		return nil
	case string:
		parsed, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return err
		}
		*zt = ZeroTime(parsed)
		return nil
	}

	return fmt.Errorf("cannot convert %T to ZeroTime", value)
}

// Value implements the [driver.Valuer] interface.
func (zf ZeroFloat64) Value() (driver.Value, error) {
	return float64(zf), nil
}

// Scan implements the [Scanner] interface.
func (zf *ZeroFloat64) Scan(value any) error {
	if value == nil {
		*zf = 0
		return nil
	}

	switch v := value.(type) {
	case float64:
		*zf = ZeroFloat64(v)
		return nil
	case float32:
		*zf = ZeroFloat64(v)
		return nil
	}

	return fmt.Errorf("cannot convert %T to ZeroInt64", value)
}

// Value implements the [driver.Valuer] interface.
func (zs ZeroString) Value() (driver.Value, error) {
	return string(zs), nil
}

// Scan implements the [Scanner] interface.
func (zs *ZeroString) Scan(value any) error {
	if value == nil {
		*zs = ""
		return nil
	}

	switch v := value.(type) {
	case []byte:
		*zs = ZeroString(v)
		return nil
	case string:
		*zs = ZeroString(v)
		return nil
	}

	return fmt.Errorf("cannot convert %T to ZeroString", value)
}

// Value implements the [driver.Valuer] interface.
func (zi ZeroInt64) Value() (driver.Value, error) {
	return int64(zi), nil
}

// Scan implements the [Scanner] interface.
func (zi *ZeroInt64) Scan(value any) error {
	if value == nil {
		*zi = 0
		return nil
	}

	if v, ok := value.(int64); ok {
		*zi = ZeroInt64(v)
		return nil
	}

	return fmt.Errorf("cannot convert %T to ZeroInt64", value)
}

// Value implements the [driver.Valuer] interface.
func (zb ZeroBool) Value() (driver.Value, error) {
	return bool(zb), nil
}

// Scan implements the [Scanner] interface.
func (zb *ZeroBool) Scan(value any) error {
	if value == nil {
		*zb = false
		return nil
	}

	if v, ok := value.(bool); ok {
		*zb = ZeroBool(v)
		return nil
	}

	return fmt.Errorf("cannot convert %T to ZeroBool", value)
}
