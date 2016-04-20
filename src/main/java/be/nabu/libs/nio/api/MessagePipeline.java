package be.nabu.libs.nio.api;

import java.util.Queue;

public interface MessagePipeline<T, R> extends Pipeline {
	/**
	 * A queue that contains all the pending requests
	 */
	public Queue<T> getRequestQueue();
	/**
	 * A queue that contains all the pending responses
	 */
	public Queue<R> getResponseQueue();
	/**
	 * The maximum size of the request queue
	 * A value of 0 means unlimited
	 */
	public int getRequestLimit();
	/**
	 * The maximum size of the response queue
	 * A value of 0 means unlimited
	 */
	public int getResponseLimit();
}
