package gob

import (
	"birpc"
	"birpc/encodingtest"
	"testing"
)

// Gob encoding itself placed in birpc package because at least one encoding impl required for birpc package internal tests
func TestGobEncoding(t *testing.T) {
	(&encodingtest.EncodingTestSuite{Encoding: birpc.GobEncoding}).Run(t)
}
