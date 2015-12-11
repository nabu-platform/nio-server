package be.nabu.libs.nio.impl;

import java.nio.channels.SocketChannel;

import be.nabu.libs.nio.api.ConnectionAcceptor;
import be.nabu.libs.nio.api.NIOServer;

public class CombinedConnectionsAcceptor implements ConnectionAcceptor {

	private ConnectionAcceptor[] acceptors;

	public CombinedConnectionsAcceptor(ConnectionAcceptor...acceptors) {
		this.acceptors = acceptors;
	}
	
	@Override
	public boolean accept(NIOServer server, SocketChannel newConnection) {
		for (ConnectionAcceptor acceptor : acceptors) {
			if (!acceptor.accept(server, newConnection)) {
				return false;
			}
		}
		return true;
	}

}
