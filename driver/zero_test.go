package driver

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type mockZeroStruct struct {
	ZT ZeroTime `json:"zt,omitzero"`
}

func TestZeroJson(t *testing.T) {
	t.Parallel()

	mz := mockZeroStruct{}
	assert.NoError(t, json.Unmarshal([]byte(`{}`), &mz))

	assert.Equal(t, ZeroTime{}, mz.ZT)
	assert.True(t, mz.ZT.IsZero())

	mz = mockZeroStruct{}
	assert.NoError(t, json.Unmarshal([]byte(`{"zt":""}`), &mz))

	assert.Equal(t, ZeroTime{}, mz.ZT)
	assert.True(t, mz.ZT.IsZero())

	mz = mockZeroStruct{}
	assert.NoError(t, json.Unmarshal([]byte(`{"zt":0}`), &mz))

	assert.Equal(t, ZeroTime{}, mz.ZT)
	assert.True(t, mz.ZT.IsZero())

	now := time.Now()
	mz = mockZeroStruct{ZT: ZeroTime(now)}
	data, err := json.Marshal(mz)

	nowStr, _ := json.Marshal(now)
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf(`{"zt":%s}`, string(nowStr)), string(data))

	mz = mockZeroStruct{}
	assert.True(t, mz.ZT.IsZero())
	assert.NoError(t, json.Unmarshal(data, &mz))
	assert.Equal(t, now.UnixMilli(), time.Time(mz.ZT).UnixMilli())
}
