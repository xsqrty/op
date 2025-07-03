package driver

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type mockZeroStruct struct {
	ZT ZeroTime `json:"zt,omitzero"`
}

func TestZeroJson(t *testing.T) {
	mz := mockZeroStruct{}
	require.NoError(t, json.Unmarshal([]byte(`{}`), &mz))

	require.Equal(t, ZeroTime{}, mz.ZT)
	require.True(t, mz.ZT.IsZero())

	mz = mockZeroStruct{}
	require.NoError(t, json.Unmarshal([]byte(`{"zt":""}`), &mz))

	require.Equal(t, ZeroTime{}, mz.ZT)
	require.True(t, mz.ZT.IsZero())

	mz = mockZeroStruct{}
	require.NoError(t, json.Unmarshal([]byte(`{"zt":0}`), &mz))

	require.Equal(t, ZeroTime{}, mz.ZT)
	require.True(t, mz.ZT.IsZero())

	now := time.Now()
	mz = mockZeroStruct{ZT: ZeroTime(now)}
	data, err := json.Marshal(mz)

	nowStr, _ := json.Marshal(now)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf(`{"zt":%s}`, string(nowStr)), string(data))

	mz = mockZeroStruct{}
	require.True(t, mz.ZT.IsZero())
	require.NoError(t, json.Unmarshal(data, &mz))
	require.Equal(t, now.UnixMilli(), time.Time(mz.ZT).UnixMilli())
}
