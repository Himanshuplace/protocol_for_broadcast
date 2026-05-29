// Package tls provides TLS certificate generation utilities for the benchmark platform.
//
// All HTTPS/WSS/HTTP2/HTTP3/QUIC/WebTransport transports need TLS.
// Rather than requiring pre-generated certificates, this package generates
// self-signed ECDSA P-256 certificates at server startup.
//
// Why ECDSA P-256?
//   - Smaller key size than RSA (256 bits vs 2048 bits) → faster TLS handshake
//   - P-256 is natively accelerated on Intel (Ivy Bridge+) and AMD (Zen+) via
//     hardware multiplication in the Montgomery domain
//   - Go's stdlib crypto/elliptic uses assembly-optimized P-256 (not generic code)
//   - TLS 1.3 (the minimum we enforce) supports ECDHE with P-256 natively
package tls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// GenerateSelfSigned generates a self-signed ECDSA P-256 TLS certificate.
// The certificate:
//   - Is valid for 10 years (benchmark-only; not for production use)
//   - Includes localhost and 127.0.0.1 as Subject Alternative Names
//   - Uses TLS 1.3-compatible signature algorithm (ECDSA with SHA-256)
//   - Additional hosts (IP or DNS) can be passed as extraHosts
//
// Typical call time: ~1ms (dominated by EC key generation, not randomness).
func GenerateSelfSigned(extraHosts ...string) (tls.Certificate, error) {
	// Generate ECDSA P-256 private key
	// Go's P-256 implementation uses assembly on AMD64 (src/crypto/internal/nistec)
	// making this ~3× faster than P-384 and ~10× faster than RSA-2048 key generation.
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}

	// Generate a random 128-bit serial number
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, err
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			Organization: []string{"Benchmark Lab"},
			CommonName:   "benchmark.local",
		},
		NotBefore: time.Now().Add(-1 * time.Minute), // allow for slight clock skew
		NotAfter:  time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:  x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth, // needed for mutual TLS scenarios
		},
		BasicConstraintsValid: true,
		// Default SANs
		IPAddresses: []net.IP{
			net.ParseIP("127.0.0.1"),
			net.ParseIP("::1"),
		},
		DNSNames: []string{"localhost", "benchmark.local"},
	}

	// Add any extra hosts
	for _, host := range extraHosts {
		if ip := net.ParseIP(host); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, host)
		}
	}

	// Self-sign: issuer == subject
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, err
	}

	return tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	)
}

// SavePEM writes cert.pem and key.pem files to dir.
// cert.pem is world-readable (0644); key.pem is owner-readable only (0600).
func SavePEM(cert tls.Certificate, dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leaf.Raw})
	if err := os.WriteFile(filepath.Join(dir, "cert.pem"), certPEM, 0644); err != nil {
		return err
	}

	ecKey, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		return nil // non-ECDSA key — skip saving (RSA keys need different marshal)
	}
	keyDER, err := x509.MarshalECPrivateKey(ecKey)
	if err != nil {
		return err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return os.WriteFile(filepath.Join(dir, "key.pem"), keyPEM, 0600)
}

// LoadOrGenerate attempts to load cert.pem + key.pem from dir.
// If either file is missing, generates a new certificate and saves it.
// This is the standard entry point for all benchmark servers.
func LoadOrGenerate(dir string, extraHosts ...string) (tls.Certificate, error) {
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")

	if _, err := os.Stat(certPath); err == nil {
		if _, err := os.Stat(keyPath); err == nil {
			return tls.LoadX509KeyPair(certPath, keyPath)
		}
	}

	cert, err := GenerateSelfSigned(extraHosts...)
	if err != nil {
		return tls.Certificate{}, err
	}
	if err := SavePEM(cert, dir); err != nil {
		// Non-fatal: we still have the cert in memory
		_ = err
	}
	return cert, nil
}

// ServerTLSConfig returns a *tls.Config suitable for benchmark servers.
// Enforces TLS 1.3 minimum (required by QUIC/HTTP3).
// nextProtos specifies ALPN tokens (e.g., "h3" for HTTP/3, "benchmark" for raw QUIC).
func ServerTLSConfig(cert tls.Certificate, nextProtos ...string) *tls.Config {
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		// CurvePreferences: prefer X25519 (faster DH) then P-256
		CurvePreferences: []tls.CurveID{
			tls.X25519,
			tls.CurveP256,
		},
	}
	if len(nextProtos) > 0 {
		cfg.NextProtos = nextProtos
	}
	return cfg
}

// ClientTLSConfig returns a *tls.Config for benchmark clients connecting to
// servers using self-signed certificates.
//
// InsecureSkipVerify: true is intentional — self-signed certs are expected.
// This is only safe in a controlled benchmark environment.
func ClientTLSConfig(nextProtos ...string) *tls.Config {
	cfg := &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // intentional for benchmark
		MinVersion:         tls.VersionTLS13,
		CurvePreferences: []tls.CurveID{
			tls.X25519,
			tls.CurveP256,
		},
	}
	if len(nextProtos) > 0 {
		cfg.NextProtos = nextProtos
	}
	return cfg
}
