package be.nabu.libs.nio.api;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.concurrent.Future;

import javax.net.ssl.SSLContext;

import be.nabu.utils.io.SSLServerMode;

public interface Server {
	public void start() throws IOException;
	public void stop();
	public Future<?> submitIOTask(Runnable runnable);
	public Future<?> submitProcessTask(Runnable runnable);
	public void close(SelectionKey key);
	public SSLContext getSSLContext();
	public SSLServerMode getSSLServerMode();
	public void setWriteInterest(SelectionKey key, boolean isInterested);
	public PipelineFactory getPipelineFactory();
}
