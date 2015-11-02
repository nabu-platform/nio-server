package be.nabu.libs.nio.api;

/**
 * The keep alive decider will decide whether or not to keep the connection alive after the response has been sent
 */
public interface KeepAliveDecider<T> {
	public boolean keepConnectionAlive(T response);
}
