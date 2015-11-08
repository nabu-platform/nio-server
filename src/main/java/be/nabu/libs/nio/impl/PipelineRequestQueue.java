package be.nabu.libs.nio.impl;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PipelineRequestQueue<T> extends ConcurrentLinkedQueue<T> implements Closeable {

	private static final long serialVersionUID = 1L;
	private MessagePipelineImpl<?, ?> pipeline;
	private boolean closed;

	PipelineRequestQueue(MessagePipelineImpl<?, ?> pipeline) {
		this.pipeline = pipeline;
	}
	
	@Override
	public boolean add(T e) {
		if (closed) {
			throw new IllegalStateException("Can not add an item to this queue, it is closed");
		}
		if (super.add(e)) {
			pipeline.process();
			return true;
		}
		return false;
	}

	@Override
	public boolean offer(T e) {
		if (closed) {
			throw new IllegalStateException("Can not add an item to this queue, it is closed");
		}
		if (super.offer(e)) {
			pipeline.process();
			return true;
		}
		return false;
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		if (closed) {
			throw new IllegalStateException("Can not add an item to this queue, it is closed");
		}
		if (super.addAll(c)) {
			pipeline.process();
			return true;
		}
		return false;
	}

	@Override
	public void close() {
		closed = true;
	}

}
