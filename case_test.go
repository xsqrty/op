package op

import (
	"testing"
)

func TestIf(t *testing.T) {
	runCases(t, []testCase{
		{
			builder: If(
				Gte("age", 70), Value("old"),
			).
				ElseIf(Gte("age", 30), Value("middle")).
				Else(Value("other")),
			expectedSql:  `CASE WHEN "age" >= ? THEN ? WHEN "age" >= ? THEN ? ELSE ? END`,
			expectedArgs: []any{70, "old", 30, "middle", "other"},
		},
		{
			builder:      If(Gte("unsafe+name", 70), Value("old")).Else(Value("other")),
			expectedSql:  "",
			expectedArgs: []any(nil),
			expectedErr:  `target "unsafe+name" contains illegal character '+'`,
		},
		{
			builder:      If(Gte("name", 70), Column("unsafe+name")).Else(Value("other")),
			expectedSql:  "",
			expectedArgs: []any(nil),
			expectedErr:  `target "unsafe+name" contains illegal character '+'`,
		},
		{
			builder:      If(Gte("name", 70), Value(100)).Else(Column("unsafe+name")),
			expectedSql:  "",
			expectedArgs: []any(nil),
			expectedErr:  `target "unsafe+name" contains illegal character '+'`,
		},
	})
}
