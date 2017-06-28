package json

import (
	"birpc/encodingtest"
	"testing"
)

//TODO: think about packages

func TestJsonEncoding(t *testing.T) {
	(&encodingtest.EncodingTestSuite{Encoding: Encoding}).Run(t)
}
