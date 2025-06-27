package op

import (
	"github.com/xsqrty/op/driver"
	"github.com/xsqrty/op/internal/testutil"
	"testing"
)

var options *driver.SqlOptions

func TestMain(m *testing.M) {
	options = testutil.NewDefaultOptions()
	m.Run()
}
