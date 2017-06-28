package birpc

import (
	"errors"
	"time"
)

type Rpc struct {
	Config *Config

	//private
	encoding *Encoding
}

func (rpc *Rpc) NewClient() *Client {
	return newClient(rpc, &TcpNetwork{})
}

func (rpc *Rpc) NewServer(id string) *Server {
	return newServer(rpc, id, &TcpNetwork{})
}

var DEFAULT_CONFIG = &Config{
	KeepAliveTimeout:  40 * time.Second,
	KeepAliveInterval: 20 * time.Second,
	Timeout:           1 * time.Minute,
}

func NewRpc(encoding *Encoding, config *Config) (*Rpc, error) {
	if encoding == nil || config == nil {
		return nil, errors.New("encoding and config must not be nil")
	}
	err := config.Validate()
	if err != nil {
		return nil, err
	}
	return &Rpc{encoding: encoding, Config: config}, nil
}
