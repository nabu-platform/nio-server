package be.nabu.libs.nio;

import be.nabu.libs.nio.api.ConnectionAcceptor;
import be.nabu.libs.nio.impl.CombinedConnectionsAcceptor;
import be.nabu.libs.nio.impl.MaxClientConnectionsAcceptor;
import be.nabu.libs.nio.impl.MaxTotalConnectionsAcceptor;

public class NIOServerUtils {
	public static ConnectionAcceptor maxConnectionsPerClient(int amountOfConnections) {
		return new MaxClientConnectionsAcceptor(amountOfConnections);
	}
	
	public static ConnectionAcceptor maxTotalConnections(int amountOfConnections) {
		return new MaxTotalConnectionsAcceptor(amountOfConnections);
	}
	
	public static ConnectionAcceptor combine(ConnectionAcceptor...acceptors) {
		return new CombinedConnectionsAcceptor(acceptors);
	}
}
