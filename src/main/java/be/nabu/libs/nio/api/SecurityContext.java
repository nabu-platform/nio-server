package be.nabu.libs.nio.api;

import java.security.cert.Certificate;

import javax.net.ssl.SSLContext;

import be.nabu.utils.io.SSLServerMode;

public interface SecurityContext {
	public SSLContext getSSLContext();
	public SSLServerMode getSSLServerMode();
	public Certificate[] getPeerCertificates();
}
