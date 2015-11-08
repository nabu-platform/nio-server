package be.nabu.libs.nio.api;

import java.nio.channels.SocketChannel;

public interface ConnectionAcceptor {
	public boolean accept(NIOServer server, SocketChannel newConnection);
}
