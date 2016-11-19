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
