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

package be.nabu.libs.nio.impl.udp;

import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelectionKey;

public class UDPSelectionKey extends AbstractSelectionKey {

	private UDPChannel channel;
	private Selector selector;

	protected UDPSelectionKey(UDPServerImpl server, SocketAddress target) {
		this.selector = server.getSelector();
		this.channel = new UDPChannel(server, this, target);
	}
	
	@Override
	public UDPChannel channel() {
		return channel;
	}

	@Override
	public Selector selector() {
		return selector;
	}

	@Override
	public int interestOps() {
		return SelectionKey.OP_READ;
	}

	@Override
	public SelectionKey interestOps(int ops) {
		return this;
	}

	@Override
	public int readyOps() {
		return SelectionKey.OP_READ;
	}

}
