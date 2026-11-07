package outbound

import (
	"crypto/tls"
	"fmt"
)

// DeliveryInfo records the transport used for one recipient delivery attempt.
type DeliveryInfo struct {
	RemoteHost     string
	TLSUsed        bool
	TLSMode        string
	TLSVersion     string
	TLSCipherSuite string
}

func tlsVersionName(version uint16) string {
	switch version {
	case tls.VersionTLS10:
		return "TLS 1.0"
	case tls.VersionTLS11:
		return "TLS 1.1"
	case tls.VersionTLS12:
		return "TLS 1.2"
	case tls.VersionTLS13:
		return "TLS 1.3"
	default:
		return fmt.Sprintf("TLS 0x%04x", version)
	}
}

func deliveryInfoFromSession(session *smtpSession, remoteHost string, mode TLSMode) DeliveryInfo {
	info := DeliveryInfo{RemoteHost: remoteHost, TLSMode: string(mode)}
	if session == nil || session.client == nil {
		return info
	}
	state, ok := session.client.TLSConnectionState()
	if !ok {
		return info
	}
	info.TLSUsed = true
	info.TLSVersion = tlsVersionName(state.Version)
	info.TLSCipherSuite = tls.CipherSuiteName(state.CipherSuite)
	return info
}
