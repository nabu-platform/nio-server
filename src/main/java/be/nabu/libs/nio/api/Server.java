package be.nabu.libs.nio.api;

import java.io.IOException;

import javax.net.ssl.SSLContext;

import be.nabu.libs.events.api.EventDispatcher;
import be.nabu.libs.metrics.api.MetricInstance;
import be.nabu.utils.io.SSLServerMode;

public interface Server {
	public void start() throws IOException;
	public void stop();
	public SSLContext getSSLContext();
	public SSLServerMode getSSLServerMode();
	public EventDispatcher getDispatcher();
	public MetricInstance getMetrics();
	public void setMetrics(MetricInstance metrics);
}
