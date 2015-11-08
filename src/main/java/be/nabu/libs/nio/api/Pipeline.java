package be.nabu.libs.nio.api;

import java.io.Closeable;

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
}
