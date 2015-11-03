package be.nabu.libs.nio.impl;

import java.io.Closeable;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import be.nabu.libs.nio.api.MessageFormatter;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.ReadableContainer;
import be.nabu.utils.io.api.WritableContainer;

public class ResponseWriter<T> implements Closeable, Runnable {

	private Logger logger = LoggerFactory.getLogger(getClass());
	private ByteBuffer buffer = IOUtils.newByteBuffer(4096, true);
	private WritableContainer<ByteBuffer> output;
	private MessagePipelineImpl<?, T> pipeline;
	private ReadableContainer<ByteBuffer> readable;
	private boolean keepAlive = true;
	
	ResponseWriter(MessagePipelineImpl<?, T> pipeline, WritableContainer<ByteBuffer> output) {
		this.pipeline = pipeline;
		this.output = output;
	}
	
	@Override
	public void close() {
		if (readable != null) {
			try {
				readable.close();
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
				T response = null;
				synchronized(output) {
					try {
						// still needs to be flushed, stop, it will be triggered again when write becomes available
						if (!flush()) {
							break;
						}
						else if (!keepAlive) {
							close();
							break;
						}
					}
					catch (IOException e) {
						logger.error("Could not flush response data", e);
						close();
						break;
					}
					response = pipeline.getResponseQueue().poll();
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
			}
		}
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

}
