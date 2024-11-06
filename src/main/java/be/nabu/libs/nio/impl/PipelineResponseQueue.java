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
				metrics.increment(METRIC_RESPONSES + ":" + NIOServerImpl.getUserId(pipeline.getSourceContext().getSocketAddress()), 1);
			}
			// for a while it seemed websocket messages were arriving out of sync, new messages triggered the sending of old replies (it seems)
			// this could however not be reproduced but if it can be, it could be interesting to use the force boolean with the following value:
			// !pipeline.getResponseWriter().isWriting()
			// the writing boolean keeps track of whether or not at any given point in time, the response writer will pick up a new message (even if the future is still running)
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
				metrics.increment(METRIC_RESPONSES + ":" + NIOServerImpl.getUserId(pipeline.getSourceContext().getSocketAddress()), 1);
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
				metrics.increment(METRIC_RESPONSES + ":" + NIOServerImpl.getUserId(pipeline.getSourceContext().getSocketAddress()), c.size());
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
