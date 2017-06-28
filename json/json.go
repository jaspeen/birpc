package json

import (
	"encoding/json"
	"github.com/jaspeen/birpc"
	"io"
)

type jsonEncoder struct {
	encoder *json.Encoder
}

func (enc *jsonEncoder) Encode(from interface{}) error {
	err := enc.encoder.Encode(from)
	if err != nil {
		if err == io.EOF {
			return err
		}
		if _, ok := err.(*birpc.RpcConnectionError); !ok {
			return &birpc.RpcError{Code: birpc.RPC_WRONG_ENCODING, Msg: err.Error()}
		}
		return err
	}
	return nil
}

type jsonDecoder struct {
	decoder *json.Decoder
}

func (dec *jsonDecoder) Decode(to interface{}) error {
	err := dec.decoder.Decode(to)
	if err != nil {
		if err == io.EOF {
			return err
		}
		if _, ok := err.(*birpc.RpcConnectionError); !ok {
			return &birpc.RpcError{Code: birpc.RPC_WRONG_ENCODING, Msg: err.Error()}
		}
		return err
	}
	return nil
}

var Encoding = &birpc.Encoding{Encoder: func(writer io.Writer) birpc.Encoder {
	return &jsonEncoder{json.NewEncoder(&birpc.ErrorPropagationWriter{Writer: writer})}
},
	Decoder: func(reader io.Reader) birpc.Decoder {
		return &jsonDecoder{json.NewDecoder(&birpc.ErrorPropagationReader{Reader: reader})}
	},
	Register: func(dataType interface{}) {
		// no need for json
	}}
