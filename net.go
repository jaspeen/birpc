package birpc

import (
	"context"
	"crypto/tls"
	"net"
)

type Network interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
	Listen(network, address string) (net.Listener, error)
}

type TcpNetwork struct{}

func (tcpnet *TcpNetwork) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, network, address)
}

func (tcpnet *TcpNetwork) Listen(network, address string) (net.Listener, error) {
	return net.Listen(network, address)
}

type TlsNetwork struct {
	ClientConfig *tls.Config
	ServerConfig *tls.Config
}

func (tlsnet *TlsNetwork) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return tls.Dial(network, address, tlsnet.ClientConfig)
}

func (tlsnet *TlsNetwork) Listen(network, address string) (net.Listener, error) {
	return tls.Listen(network, address, tlsnet.ServerConfig)
}
