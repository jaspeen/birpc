package birpc

import (
	"encoding/gob"
	"io"
)

type gobEncoder struct {
	encoder *gob.Encoder
}

func (enc *gobEncoder) Encode(from interface{}) error {
	err := enc.encoder.Encode(from)
	if err != nil {
		if err == io.EOF {
			return err
		}
		if _, ok := err.(*RpcConnectionError); !ok {
			return &RpcError{Code: RPC_WRONG_ENCODING, Msg: err.Error()}
		}
		return err
	}
	return nil
}

type gobDecoder struct {
	decoder *gob.Decoder
}

func (dec *gobDecoder) Decode(to interface{}) error {
	err := dec.decoder.Decode(to)
	if err != nil {
		if err == io.EOF {
			return err
		}
		if _, ok := err.(*RpcConnectionError); !ok {
			return &RpcError{Code: RPC_WRONG_ENCODING, Msg: err.Error()}
		}
		return err
	}
	return nil
}

var GobEncoding = &Encoding{Encoder: func(writer io.Writer) Encoder {
	return &gobEncoder{gob.NewEncoder(&ErrorPropagationWriter{Writer: writer})}
},
	Decoder: func(reader io.Reader) Decoder {
		return &gobDecoder{gob.NewDecoder(&ErrorPropagationReader{Reader: reader})}
	},
	Register: func(dataType interface{}) {
		gob.Register(dataType)
	}}
