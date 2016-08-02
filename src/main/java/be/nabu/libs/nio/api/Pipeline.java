package be.nabu.libs.nio.api;

import java.io.Closeable;
import java.util.Date;

/**
 * A data pipeline is a two way pipeline where data can arrive from an external source or be sent to an external source
 * In the following, the 'source' is considered for example the remote client in a socket connection
 */
public interface Pipeline extends Closeable {
	/**
	 * Content has arrived from the source, trigger a new read on the pipeline to process it
	 */
	public void read();
	/**
	 * The source is ready to receive more data, trigger a new write on the channel
	 */
	public void write();
	/**
	 * The server this pipeline belongs to
	 */
	public NIOServer getServer();
	/**
	 * Get the security context for this data pipeline 
	 */
	public SecurityContext getSecurityContext();
	/**
	 * Return the context related to the source
	 */
	public SourceContext getSourceContext();
	/**
	 * The current state of the pipeline
	 */
	public PipelineState getState();
	/**
	 * If a request takes too long to come in, we want to time out the connection to prevent slow write "attacks" taking up resources
	 * If set to 0 the timeout is infinite
	 */
	public long getReadTimeout();
	/**
	 * If a response takes too long to write to the target, we want to time out the connection to prevent slow read "attacks" from taking up resources
	 * If set to 0 the timeout is infinite
	 */
	public long getWriteTimeout();
	/**
	 * When the pipeline last saw incoming data (indicating incoming activity)
	 */
	public Date getLastRead();
	/**
	 * When the pipeline last saw outgoing data (indicating outgoing activity)
	 */
	public Date getLastWritten();
}
