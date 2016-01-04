package be.nabu.libs.nio.impl;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import be.nabu.libs.metrics.api.MetricInstance;

public class PipelineRequestQueue<T> extends ConcurrentLinkedQueue<T> implements Closeable {

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
		if (super.add(e)) {
			MetricInstance metrics = pipeline.getServer().getMetrics();
			if (metrics != null) {
				metrics.increment(METRIC_REQUESTS + ":" + NIOServerImpl.getUserId(pipeline.getSourceContext().getSocket()), 1);
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
		if (super.offer(e)) {
			MetricInstance metrics = pipeline.getServer().getMetrics();
			if (metrics != null) {
				metrics.increment(METRIC_REQUESTS + ":" + NIOServerImpl.getUserId(pipeline.getSourceContext().getSocket()), 1);
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
		if (super.addAll(c)) {
			MetricInstance metrics = pipeline.getServer().getMetrics();
			if (metrics != null) {
				metrics.increment(METRIC_REQUESTS + ":" + NIOServerImpl.getUserId(pipeline.getSourceContext().getSocket()), c.size());
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
