package be.nabu.libs.nio.impl;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.libs.metrics.api.MetricInstance;
import be.nabu.libs.metrics.api.MetricTimer;
import be.nabu.libs.nio.api.MessageFormatter;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.ReadableContainer;
import be.nabu.utils.io.api.WritableContainer;
import be.nabu.utils.io.containers.CountingWritableContainerImpl;

public class ResponseWriter<T> implements Closeable, Runnable {

	public static final String FORMAT_TIME = "formatTime";
	public static final String RESPONSE_SIZE = "responseSize";
	public static final String TRANSFER_RATE = "responseTransferRate";
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	private ByteBuffer buffer = IOUtils.newByteBuffer(4096, true);
	private CountingWritableContainerImpl<ByteBuffer> output;
	private MessagePipelineImpl<?, T> pipeline;
	private ReadableContainer<ByteBuffer> readable;
	private boolean keepAlive = true;
	private MetricTimer timer;
	private Date started;
	
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
			catch (Exception e) {
				logger.error("Unable to close readable", e);
			}
		}
		pipeline.close();
	}

	@Override
	public void run() {
		pipeline.putMDCContext();
		// either it's a socketchannel and connected, or the channel is at least open
		boolean open = (pipeline.getChannel() instanceof SocketChannel && ((SocketChannel) pipeline.getChannel()).isConnected())
			|| (!(pipeline.getChannel() instanceof SocketChannel) && pipeline.getChannel().isOpen());
		if (open && !pipeline.isClosed()) {
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
						String userId = NIOServerImpl.getUserId(pipeline.getSourceContext().getSocketAddress());
						timer.getMetrics().log(RESPONSE_SIZE + ":" + userId, output.getWrittenTotal());
						long transferRate = output.getWrittenTotal() / timer.getTimeUnit().convert(timed, TimeUnit.MILLISECONDS);
						timer.getMetrics().log(TRANSFER_RATE + ":" + userId, transferRate);
						timer = null;
					}
					if (!keepAlive) {
						close();
						return false;
					}
				}
			}
			catch (IOException e) {
				logger.debug("Could not flush response data", e);
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
				timer = metrics.start(FORMAT_TIME + ":" + NIOServerImpl.getUserId(pipeline.getSourceContext().getSocketAddress()));
			}
			// reset written amount, even if not using metrics, we don't want it to overflow etc
			output.setWrittenTotal(0);
			
			started = new Date();
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
		boolean open = (pipeline.getChannel() instanceof SocketChannel && ((SocketChannel) pipeline.getChannel()).isConnected() && !((SocketChannel) pipeline.getChannel()).socket().isOutputShutdown())
			|| (!(pipeline.getChannel() instanceof SocketChannel) && pipeline.getChannel().isOpen());
		
		if (!pipeline.isClosed() && open) {
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
							logger.debug("Could not close readable", e);
						}
						readable = null;
					}
				}
			}
			// still data in the buffer or the origin, add an interest in write ops so we can complete this write
			if (buffer.remainingData() > 0 || readable != null) {
				if (started != null && pipeline.getWriteTimeout() > 0 && started.getTime() < new Date().getTime() - pipeline.getWriteTimeout()) {
					logger.warn("Write timed out, started at {} with a timeout value of {}", started, pipeline.getWriteTimeout());
					pipeline.close();
				}
				else {
					logger.debug("Not all response content could be written, rescheduling the writer for: {}", pipeline);
					// make sure we can reschedule it and no one can reschedule while we toggle the boolean
					pipeline.registerWriteInterest();
				}
				return false;
			}
			// deregister interest in write ops otherwise it will cycle endlessly (it is almost always writable)
			else {
				started = null;
				logger.trace("All response data written, removing write interest for: {}", pipeline);
				pipeline.unregisterWriteInterest();
				output.flush();
				return true;
			}
		}
		else {
			logger.debug("Skipping flush (closed: " + pipeline.isClosed() + ", open: " + open + ")");
			close();
			return false;
		}
	}

	public boolean isDone() {
		return pipeline.getResponseQueue().isEmpty() && readable == null;
	}
}
