package be.nabu.libs.nio.api;

public enum PipelineState {
	/**
	 * The pipeline has requests pending or responses or is writing something out
	 */
	RUNNING,
	/**
	 * The pipeline has no pending requests or responses and any writing is done
	 * It is open to accepting new requests
	 */
	WAITING,
	/**
	 * No new data should be pushed to it but it may still process pending requests and write out response data 
	 */
	DRAINING,
	/**
	 * The pipeline is closed, it will not process new data or write out any responses, it may still process pending requests
	 */
	CLOSED
}
