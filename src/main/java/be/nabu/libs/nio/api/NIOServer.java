package be.nabu.libs.nio.api;

import java.nio.channels.SelectionKey;
import java.util.Collection;
import java.util.concurrent.Future;

/**
 * The server will dispatch events concerning new connections and closed connections over the event dispatcher
 */
public interface NIOServer extends Server {
	public Future<?> submitIOTask(Runnable runnable);
	public Future<?> submitProcessTask(Runnable runnable);
	public void close(SelectionKey key);
	public void setWriteInterest(SelectionKey key, boolean isInterested);
	public void upgrade(SelectionKey key, Pipeline pipeline);
	public PipelineFactory getPipelineFactory();
	public Collection<Pipeline> getPipelines();
	public ConnectionAcceptor getConnectionAcceptor();
	public void setConnectionAcceptor(ConnectionAcceptor connectionAcceptor);
}
