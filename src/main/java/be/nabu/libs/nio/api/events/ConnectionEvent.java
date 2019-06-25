package be.nabu.libs.nio.api.events;

import be.nabu.libs.nio.api.NIOServer;
import be.nabu.libs.nio.api.Pipeline;

public interface ConnectionEvent {
	
	public enum ConnectionState {
		CONNECTED,
		REJECTED,
		CLOSED,
		// after the connection has been closed and no additional data in the input, we send out an empty event 
		EMPTY,
		UPGRADED
	}
	
	public NIOServer getServer();
	public ConnectionState getState();
	public Pipeline getPipeline();
}
