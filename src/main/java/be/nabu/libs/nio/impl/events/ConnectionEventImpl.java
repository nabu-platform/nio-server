package be.nabu.libs.nio.impl.events;

import be.nabu.libs.nio.api.NIOServer;
import be.nabu.libs.nio.api.Pipeline;
import be.nabu.libs.nio.api.events.ConnectionEvent;

public class ConnectionEventImpl implements ConnectionEvent {

	private ConnectionState status;
	private Pipeline pipeline;
	private NIOServer server;

	public ConnectionEventImpl(NIOServer server, Pipeline pipeline, ConnectionState status) {
		this.server = server;
		this.pipeline = pipeline;
		this.status = status;
	}

	@Override
	public ConnectionState getState() {
		return status;
	}

	@Override
	public Pipeline getPipeline() {
		return pipeline;
	}

	@Override
	public NIOServer getServer() {
		return server;
	}
	
}
