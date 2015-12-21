package be.nabu.libs.nio.impl;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import be.nabu.libs.metrics.api.MetricInstance;
import be.nabu.libs.metrics.api.MetricTimer;
import be.nabu.libs.nio.api.MessageFormatter;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.ReadableContainer;
import be.nabu.utils.io.api.WritableContainer;
import be.nabu.utils.io.containers.CountingWritableContainerImpl;

public class ResponseWriter<T> implements Closeable, Runnable {

	public static final String TOTAL_FORMAT_TIME = "totalFormatTime";
	public static final String USER_FORMAT_TIME = "userFormatTime";
	public static final String TOTAL_RESPONSE_SIZE = "totalResponseSize";
	public static final String USER_RESPONSE_SIZE = "userResponseSize";
	public static final String TOTAL_TRANSFER_RATE = "totalResponseTransferRate";
	public static final String USER_TRANSFER_RATE = "userResponseTransferRate";
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	private ByteBuffer buffer = IOUtils.newByteBuffer(4096, true);
	private CountingWritableContainerImpl<ByteBuffer> output;
	private MessagePipelineImpl<?, T> pipeline;
	private ReadableContainer<ByteBuffer> readable;
	private boolean keepAlive = true;
	private MetricTimer timer;
	
	ResponseWriter(MessagePipelineImpl<?, T> pipeline, WritableContainer<ByteBuffer> output) {
		this.pipeline = pipeline;
		this.output = new CountingWritableContainerImpl<ByteBuffer>(output);
	}
	
	@Override
	public void close() {
		if (readable != null) {
			try {
				readable.close();
				readable = null;
			}
			catch (IOException e) {
				logger.error("Unable to close readable", e);
			}
		}
		pipeline.close();
	}

	@Override
	public void run() {
		MDC.put("socket", pipeline.getChannel().socket().toString());
		if (pipeline.getChannel().isConnected() && !pipeline.isClosed()) {
			while(!Thread.interrupted()) {
				// first check if the pipeline has a parent that is draining
				if (pipeline.getParentPipeline() != null && !pipeline.getParentPipeline().getResponseWriter().isDone()) {
					if (!pipeline.getParentPipeline().getResponseWriter().write()) {
						break;
					}
				}
				else if (!write()) {
					break;
				}
			}
		}
	}

	private boolean write() {
		T response = null;
		synchronized(output) {
			try {
				// still needs to be flushed, stop, it will be triggered again when write becomes available
				if (!flush()) {
					return false;
				}
				else {
					if (timer != null) {
						long timed = timer.stop();
						String userId = NIOServerImpl.getUserId(pipeline.getSourceContext().getSocket());
						timer.getMetrics().log(USER_FORMAT_TIME + ":" + userId, timed);
						timer.getMetrics().log(TOTAL_RESPONSE_SIZE, output.getWrittenTotal());
						timer.getMetrics().log(USER_RESPONSE_SIZE + ":" + userId, output.getWrittenTotal());
						long transferRate = output.getWrittenTotal() / timer.getTimeUnit().convert(timed, TimeUnit.SECONDS);
						timer.getMetrics().log(TOTAL_TRANSFER_RATE, transferRate);
						timer.getMetrics().log(USER_TRANSFER_RATE + ":" + userId, transferRate);
						timer = null;
					}
					if (!keepAlive) {
						close();
						return false;
					}
				}
			}
			catch (IOException e) {
				logger.error("Could not flush response data", e);
				close();
				return false;
			}
			response = pipeline.getResponseQueue().poll();
			
			MetricInstance metrics = pipeline.getServer().getMetrics();
			// if no response, the queue is empty
			if (response == null) {
				return false;
			}
			else if (metrics != null) {
				timer = metrics.start(TOTAL_FORMAT_TIME);
			}
			// reset written amount, even if not using metrics, we don't want it to overflow etc
			output.setWrittenTotal(0);
			
			keepAlive = pipeline.getKeepAliveDecider().keepConnectionAlive(response);
			
			MessageFormatter<T> messageFormatter = pipeline.getResponseFormatterFactory().newMessageFormatter();
			try {
				readable = messageFormatter.format(response);
			}
			catch (Exception e) {
				logger.error("Could not format response {}", e);
				response = pipeline.getExceptionFormatter().format(null, e);
				if (response != null) {
					readable = messageFormatter.format(response);
				}
				keepAlive = false;
			}
		}
		return true;
	}
	
	private boolean flush() throws IOException {
		if (!pipeline.isClosed() && pipeline.getChannel().isConnected() && !pipeline.getChannel().socket().isOutputShutdown()) {
			// flush the buffer (if required)
			if (buffer.remainingData() == 0 || buffer.remainingData() == output.write(buffer)) {
				// try to write to the output
				if (readable != null) {
					long read = 0;
					while ((read = readable.read(buffer)) > 0) {
						// if the output is shut down, close the pipeline
						if (output.write(buffer) < 0) {
							close();
							return false;
						}
						// if we couldn't write everything out, stop
						else if (buffer.remainingData() > 0) {
							break;
						}
					}
					// if we read the end of the stream, toss it
					if (read < 0) {
						try {
							readable.close();
						}
						catch (Exception e) {
							logger.warn("Could not close readable", e);
						}
						readable = null;
					}
				}
			}
			// still data in the buffer or the origin, add an interest in write ops so we can complete this write
			if (buffer.remainingData() > 0 || readable != null) {
				logger.debug("Not all response content could be written, rescheduling the writer for: {}", pipeline);
				// make sure we can reschedule it and no one can reschedule while we toggle the boolean
				pipeline.registerWriteInterest();
				return false;
			}
			// deregister interest in write ops otherwise it will cycle endlessly (it is almost always writable)
			else {
				logger.trace("All response data written, removing write interest for: {}", pipeline);
				pipeline.unregisterWriteInterest();
				output.flush();
				return true;
			}
		}
		else {
			logger.warn("Skipping flush (closed: " + pipeline.isClosed() + ", connected: " + pipeline.getChannel().isConnected() + ", output shutdown: " + pipeline.getChannel().socket().isOutputShutdown() + ")");
			close();
			return false;
		}
	}

	public boolean isDone() {
		return pipeline.getResponseQueue().isEmpty() && readable == null;
	}
}
