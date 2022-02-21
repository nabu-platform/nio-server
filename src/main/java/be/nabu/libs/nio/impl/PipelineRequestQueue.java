package be.nabu.libs.nio.impl;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import be.nabu.libs.metrics.api.MetricInstance;

public class PipelineRequestQueue<T> extends ConcurrentLinkedQueue<T> implements Closeable {
	// a delta sink is used to do the increment, however we added automated windowing on the statistics. this means we reset the statistics for each window, but the delta sink is not reset. this in turn means the "amount" is correct but the "ema" etc is the full count since startup
	public static final String METRIC_REQUESTS = "requests";
	
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
		else if (pipeline.getRequestLimit() > 0 && size() >= pipeline.getRequestLimit()) {
			throw new IllegalStateException("The request queue is full (" + size() + ")");
		}
		if (super.add(e)) {
			MetricInstance metrics = pipeline.getServer().getMetrics();
			if (metrics != null) {
				metrics.increment(METRIC_REQUESTS + ":" + NIOServerImpl.getUserId(pipeline.getSourceContext().getSocketAddress()), 1);
			}
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
		else if (pipeline.getRequestLimit() > 0 && size() >= pipeline.getRequestLimit()) {
			throw new IllegalStateException("The request queue is full (" + size() + ")");
		}
		if (super.offer(e)) {
			MetricInstance metrics = pipeline.getServer().getMetrics();
			if (metrics != null) {
				metrics.increment(METRIC_REQUESTS + ":" + NIOServerImpl.getUserId(pipeline.getSourceContext().getSocketAddress()), 1);
			}
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
		else if (pipeline.getRequestLimit() > 0 && size() >= pipeline.getRequestLimit()) {
			throw new IllegalStateException("The request queue is full (" + size() + ")");
		}
		if (super.addAll(c)) {
			MetricInstance metrics = pipeline.getServer().getMetrics();
			if (metrics != null) {
				metrics.increment(METRIC_REQUESTS + ":" + NIOServerImpl.getUserId(pipeline.getSourceContext().getSocketAddress()), c.size());
			}
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
