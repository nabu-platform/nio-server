package be.nabu.libs.nio.impl.udp;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.WritableByteChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.ArrayDeque;
import java.util.Deque;

public class UDPChannel extends AbstractSelectableChannel implements ReadableByteChannel, WritableByteChannel, ByteChannel {

	private Deque<ByteBuffer> pending = new ArrayDeque<ByteBuffer>();
	private UDPServerImpl server;
	private SocketAddress target;
	private boolean closed;
	private UDPSelectionKey selectionKey;

	protected UDPChannel(UDPServerImpl server, UDPSelectionKey selectionKey, SocketAddress target) {
		super(server.getSelector().provider());
		this.server = server;
		this.selectionKey = selectionKey;
		this.target = target;
	}

	@Override
	protected void implCloseSelectableChannel() throws IOException {
		server.close(selectionKey);
		closed = true;
	}

	@Override
	protected void implConfigureBlocking(boolean block) throws IOException {
		// currently always non blocking
		if (block) {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	public int validOps() {
		return SelectionKey.OP_READ;
	}

	@Override
	public int write(ByteBuffer src) throws IOException {
		return server.getChannel().send(src, target);
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		if (!pending.isEmpty()) {
			synchronized(pending) {
				if (!pending.isEmpty()) {
					ByteBuffer peek = pending.peek();
					int limit = -1;
					if (peek.remaining() > dst.remaining()) {
						limit = peek.limit();
						peek.limit(limit - (peek.remaining() - dst.remaining()));
					}
					int amountToWrite = peek.remaining();
					if (peek.remaining() > 0) {
						dst.put(peek);
					}
					if (limit >= 0) {
						peek.limit(limit);
					}
					if (peek.remaining() == 0) {
						pending.pollFirst();
					}
					return amountToWrite;
				}
			}
		}
		return closed ? -1 : 0;
	}

	int counter = 0;
	void push(ByteBuffer buffer) {
		synchronized(pending) {
			pending.offer(buffer);
		}
	}
	
	public boolean hasPending() {
		return pending.size() > 0;
	}

	public SocketAddress getTarget() {
		return target;
	}

	public UDPServerImpl getServer() {
		return server;
	}
}
