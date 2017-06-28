package json

import (
	"github.com/jaspeen/birpc/encodingtest"
	"testing"
)

//TODO: think about packages

func TestJsonEncoding(t *testing.T) {
	(&encodingtest.EncodingTestSuite{Encoding: Encoding}).Run(t)
}
