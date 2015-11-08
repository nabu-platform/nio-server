package be.nabu.libs.nio.api;

import java.nio.channels.SelectionKey;
import java.util.Collection;
import java.util.concurrent.Future;

public interface NIOServer extends Server {
	public Future<?> submitIOTask(Runnable runnable);
	public Future<?> submitProcessTask(Runnable runnable);
	public void close(SelectionKey key);
	public void setWriteInterest(SelectionKey key, boolean isInterested);
	public PipelineFactory getPipelineFactory();
	public Collection<Pipeline> getPipelines();
	
	public ConnectionAcceptor getConnectionAcceptor();
	public void setConnectionAcceptor(ConnectionAcceptor connectionAcceptor);
}
