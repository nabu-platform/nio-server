package be.nabu.libs.nio.impl.events;

import java.net.Socket;

import be.nabu.libs.nio.api.events.ConnectionEvent;

public class ConnectionEventImpl implements ConnectionEvent {

	private Socket socket;
	private ConnectionState status;

	public ConnectionEventImpl(Socket socket, ConnectionState status) {
		this.socket = socket;
		this.status = status;
	}
	
	@Override
	public Socket getSocket() {
		return socket;
	}

	@Override
	public ConnectionState getState() {
		return status;
	}

}
