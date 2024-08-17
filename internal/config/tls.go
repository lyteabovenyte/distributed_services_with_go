package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
)

// TLSConfig defines the parameter that SetUpTLSConfig uses
// to determine what type of *tls.Config to return.
type TLSConfig struct {
	CertFile      string
	KeyFile       string
	CAFile        string
	ServerAddress string
	Server        bool
}

// setUpTLSConfig allows us to get each type of *tls.Config with on function call.
// Client *tls.Config is set up to verify the server’s certificate
// with the client’s by setting the *tls.Config’s RootCAs.
// Client *tls.Config is set up to verify the server’s certificate
// and allow the server to verify the client’s certificate by setting its RootCAs and its Certificates.
// Server *tls.Config is set up to verify the client’s certificate and allow the client
// to verify the server’s certificate by setting its ClientCAs, Certificate, and ClientAuth mode set to tls.RequireAndVerifyCert.
func SetUpTLSConfig(cfg TLSConfig) (*tls.Config, error) {

	var err error
	tlsConfig := &tls.Config{}

	if cfg.CertFile != "" && cfg.KeyFile != "" {
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(
			cfg.CertFile, // contains the public key.
			cfg.KeyFile,  // contains the private key.
		)
		if err != nil {
			return nil, err
		}
	}
	if cfg.CAFile != "" {
		b, err := ioutil.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		ca := x509.NewCertPool()
		ok := ca.AppendCertsFromPEM([]byte(b))
		if !ok {
			return nil, fmt.Errorf(
				"failed to parse root certificate: %q",
				cfg.CAFile,
			)
		}
		if cfg.Server {
			tlsConfig.ClientCAs = ca
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsConfig.RootCAs = ca
		}
		tlsConfig.ServerName = cfg.ServerAddress
	}
	return tlsConfig, nil
}
