package be.nabu.libs.nio.impl.events;

import be.nabu.libs.nio.api.Pipeline;
import be.nabu.libs.nio.api.events.ConnectionEvent;

public class ConnectionEventImpl implements ConnectionEvent {

	private ConnectionState status;
	private Pipeline pipeline;

	public ConnectionEventImpl(Pipeline pipeline, ConnectionState status) {
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
	
}
