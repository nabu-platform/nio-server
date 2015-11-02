package be.nabu.libs.nio.api;

import java.io.IOException;

import javax.net.ssl.SSLContext;

import be.nabu.utils.io.SSLServerMode;

public interface Server {
	public void start() throws IOException;
	public void stop();
	public SSLContext getSSLContext();
	public SSLServerMode getSSLServerMode();
}
