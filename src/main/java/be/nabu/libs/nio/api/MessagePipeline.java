package be.nabu.libs.nio.api;

import java.util.Queue;

public interface MessagePipeline<T, R> extends Pipeline {
	public Queue<T> getRequestQueue();
	public Queue<R> getResponseQueue();
}
