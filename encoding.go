package birpc

import "io"

/**
  Abstraction on rpc message encoding/decoding.
  Assuming what encoding can put/read metadata like type info etc.
    it attached to the stream right after stream header read and then stream messages read/write only via this encoding impl
  Implementation must support marshalling/unmarshalling of all base Golang types without explicit registration
*/

// Must return RpcError(code=RPC_WRONG_ENCODING) or RpcConnectionError on underlying reader/writer errors
type Encoder interface {
	Encode(from interface{}) error
}

// Must return RpcError(code=RPC_WRONG_ENCODING) or RpcConnectionError on underlying reader/writer errors
type Decoder interface {
	Decode(to interface{}) error
}

type EncodingProvider func(writer io.Writer) Encoder
type DecodingProvider func(reader io.Reader) Decoder

type Encoding struct {
	Encoder  EncodingProvider
	Decoder  DecodingProvider
	Register func(dataType interface{})
}
