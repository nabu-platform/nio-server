package be.nabu.libs.nio.impl;

import java.nio.channels.SocketChannel;

import be.nabu.libs.nio.api.ConnectionAcceptor;
import be.nabu.libs.nio.api.NIOServer;

public class MaxTotalConnectionsAcceptor implements ConnectionAcceptor {

	private int maxTotalConnections;

	public MaxTotalConnectionsAcceptor(int maxTotalConnections) {
		this.maxTotalConnections = maxTotalConnections;
	}
	
	@Override
	public boolean accept(NIOServer server, SocketChannel newConnection) {
		return server.getPipelines().size() < maxTotalConnections;
	}

}
