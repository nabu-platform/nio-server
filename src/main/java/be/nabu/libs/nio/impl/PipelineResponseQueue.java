package be.nabu.libs.nio.impl;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import be.nabu.libs.metrics.api.MetricInstance;

public class PipelineResponseQueue<T> extends ConcurrentLinkedQueue<T> implements Closeable {

	public static final String METRIC_RESPONSES = "responses";
	
	private static final long serialVersionUID = 1L;
	private MessagePipelineImpl<?, ?> pipeline;
	private boolean closed;

	PipelineResponseQueue(MessagePipelineImpl<?, ?> pipeline) {
		this.pipeline = pipeline;
	}
	
	@Override
	public boolean add(T e) {
		if (closed) {
			throw new IllegalStateException("Can not add an item to this queue, it is closed");
		}
		else if (pipeline.getResponseLimit() > 0 && size() >= pipeline.getResponseLimit()) {
			throw new IllegalStateException("The response queue is full (" + size() + ")");
		}
		if (super.add(e)) {
			MetricInstance metrics = pipeline.getServer().getMetrics();
			if (metrics != null) {
				metrics.increment(METRIC_RESPONSES + ":" + NIOServerImpl.getUserId(pipeline.getSourceContext().getSocket()), 1);
			}
			pipeline.write();
			return true;
		}
		return false;
	}

	@Override
	public boolean offer(T e) {
		if (closed) {
			throw new IllegalStateException("Can not add an item to this queue, it is closed");
		}
		else if (pipeline.getResponseLimit() > 0 && size() >= pipeline.getResponseLimit()) {
			throw new IllegalStateException("The response queue is full (" + size() + ")");
		}
		if (super.offer(e)) {
			MetricInstance metrics = pipeline.getServer().getMetrics();
			if (metrics != null) {
				metrics.increment(METRIC_RESPONSES + ":" + NIOServerImpl.getUserId(pipeline.getSourceContext().getSocket()), 1);
			}
			pipeline.write();
			return true;
		}
		return false;
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		if (closed) {
			throw new IllegalStateException("Can not add an item to this queue, it is closed");
		}
		else if (pipeline.getResponseLimit() > 0 && size() >= pipeline.getResponseLimit()) {
			throw new IllegalStateException("The response queue is full (" + size() + ")");
		}
		if (super.addAll(c)) {
			MetricInstance metrics = pipeline.getServer().getMetrics();
			if (metrics != null) {
				metrics.increment(METRIC_RESPONSES + ":" + NIOServerImpl.getUserId(pipeline.getSourceContext().getSocket()), c.size());
			}
			pipeline.write();
			return true;
		}
		return false;
	}

	@Override
	public void close() {
		closed = true;
	}

}
