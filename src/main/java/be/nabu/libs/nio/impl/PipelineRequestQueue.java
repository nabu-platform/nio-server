package be.nabu.libs.nio.impl;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PipelineRequestQueue<T> extends ConcurrentLinkedQueue<T> {

	private static final long serialVersionUID = 1L;
	private MessagePipelineImpl<?, ?> pipeline;

	PipelineRequestQueue(MessagePipelineImpl<?, ?> pipeline) {
		this.pipeline = pipeline;
	}
	
	@Override
	public boolean add(T e) {
		if (super.add(e)) {
			pipeline.process();
			return true;
		}
		return false;
	}

	@Override
	public boolean offer(T e) {
		if (super.offer(e)) {
			pipeline.process();
			return true;
		}
		return false;
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		if (super.addAll(c)) {
			pipeline.write();
			return true;
		}
		return false;
	}

}
