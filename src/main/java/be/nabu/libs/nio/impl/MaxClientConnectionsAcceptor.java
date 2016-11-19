package be.nabu.libs.nio.impl;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import be.nabu.libs.nio.api.ConnectionAcceptor;
import be.nabu.libs.nio.api.NIOServer;
import be.nabu.libs.nio.api.Pipeline;

public class MaxClientConnectionsAcceptor implements ConnectionAcceptor {

	private int amount;

	public MaxClientConnectionsAcceptor(int amount) {
		this.amount = amount;
	}
	
	@Override
	public boolean accept(NIOServer server, SocketChannel newConnection) {
		int current = 0;
		InetAddress inetAddress = newConnection.socket().getInetAddress();
		if (inetAddress == null) {
			return false;
		}
		for (Pipeline pipeline : server.getPipelines()) {
			if (pipeline.getSourceContext().getSocketAddress() != null && ((InetSocketAddress) pipeline.getSourceContext().getSocketAddress()).getAddress().getHostAddress().equals(inetAddress.getHostAddress())) {
				current++;
			}
		}
		return current < amount;
	}

}
