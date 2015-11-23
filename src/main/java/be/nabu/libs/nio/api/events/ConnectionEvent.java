package be.nabu.libs.nio.api.events;

import java.net.Socket;

public interface ConnectionEvent {
	
	public enum ConnectionState {
		CONNECTED,
		REJECTED,
		CLOSED
	}
	
	public Socket getSocket();
	public ConnectionState getState();
}
