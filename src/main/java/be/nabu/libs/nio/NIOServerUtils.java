/*
* Copyright (C) 2015 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

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
