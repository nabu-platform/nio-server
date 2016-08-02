package be.nabu.libs.nio.impl;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.cert.Certificate;
import java.util.Date;
import java.util.Queue;
import java.util.concurrent.Future;

import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.libs.nio.api.ExceptionFormatter;
import be.nabu.libs.nio.api.KeepAliveDecider;
import be.nabu.libs.nio.api.MessageFormatterFactory;
import be.nabu.libs.nio.api.MessageParserFactory;
import be.nabu.libs.nio.api.MessagePipeline;
import be.nabu.libs.nio.api.MessageProcessorFactory;
import be.nabu.libs.nio.api.PipelineState;
import be.nabu.libs.nio.api.SecurityContext;
import be.nabu.libs.nio.api.NIOServer;
import be.nabu.libs.nio.api.SourceContext;
import be.nabu.libs.nio.api.UpgradeableMessagePipeline;
import be.nabu.libs.nio.api.events.ConnectionEvent;
import be.nabu.libs.nio.impl.events.ConnectionEventImpl;
import be.nabu.utils.io.SSLServerMode;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.Container;
import be.nabu.utils.io.containers.bytes.SSLSocketByteContainer;
import be.nabu.utils.io.containers.bytes.SocketByteContainer;

public class MessagePipelineImpl<T, R> implements UpgradeableMessagePipeline<T, R> {
	
	private Date created = new Date();
	private Date lastRead, lastWritten, lastProcessed;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private Future<?> futureRead, futureWrite, futureProcess;
	private NIOServer server;
	private SelectionKey selectionKey;
	private Queue<T> requestQueue;
	private Queue<R> responseQueue;
	private SocketChannel channel;
	private SSLSocketByteContainer sslContainer;
	private Container<ByteBuffer> container;
	
	private RequestFramer<T> requestFramer;
	private ResponseWriter<R> responseWriter;
	private RequestProcessor<T, R> requestProcessor;
	private MessageFormatterFactory<R> responseFormatterFactory;
	private MessageParserFactory<T> requestParserFactory;
	private MessageProcessorFactory<T, R> messageProcessorFactory;
	private KeepAliveDecider<R> keepAliveDecider;
	private ExceptionFormatter<T, R> exceptionFormatter;
	private long readTimeout, writeTimeout;
	private int requestLimit, responseLimit;
	
	/**
	 * It is possible that this pipeline replaced an existing pipeline (for example after a connection upgrade or a live protocol switch)
	 * This is the parent pipeline that was handling the data before it was supplanted, it is mostly for allowing drainage
	 */
	private MessagePipelineImpl<?, ?> parentPipeline;
	
	private boolean closed;
	
	public MessagePipelineImpl(NIOServer server, SelectionKey selectionKey, MessageParserFactory<T> requestParserFactory, MessageFormatterFactory<R> responseFormatterFactory, MessageProcessorFactory<T, R> messageProcessorFactory, KeepAliveDecider<R> keepAliveDecider, ExceptionFormatter<T, R> exceptionFormatter) throws IOException {
		this.server = server;
		this.selectionKey = selectionKey;
		this.requestParserFactory = requestParserFactory;
		this.responseFormatterFactory = responseFormatterFactory;
		this.messageProcessorFactory = messageProcessorFactory;
		this.keepAliveDecider = keepAliveDecider;
		this.exceptionFormatter = exceptionFormatter;
		this.requestQueue = new PipelineRequestQueue<T>(this);
		this.responseQueue = new PipelineResponseQueue<R>(this);
		this.channel = (SocketChannel) selectionKey.channel();
		this.container = new SocketByteContainer(channel);
		// perform SSL if required
		if (server.getSSLContext() != null) {
			sslContainer = new SSLSocketByteContainer(container, server.getSSLContext(), server.getSSLServerMode());
			container = sslContainer;
		}
		this.requestFramer = new RequestFramer<T>(this, container);
		this.responseWriter = new ResponseWriter<R>(this, container);
		this.requestProcessor = new RequestProcessor<T, R>(this); 
	}
	
	private MessagePipelineImpl(MessagePipelineImpl<?, ?> parentPipeline, MessageParserFactory<T> requestParserFactory, MessageFormatterFactory<R> responseFormatterFactory, MessageProcessorFactory<T, R> messageProcessorFactory, KeepAliveDecider<R> keepAliveDecider, ExceptionFormatter<T, R> exceptionFormatter) {
		this.parentPipeline = parentPipeline;
		this.server = parentPipeline.getServer();
		this.selectionKey = parentPipeline.selectionKey;
		this.container = parentPipeline.container;
		this.requestParserFactory = requestParserFactory;
		this.responseFormatterFactory = responseFormatterFactory;
		this.messageProcessorFactory = messageProcessorFactory;
		this.keepAliveDecider = keepAliveDecider;
		this.exceptionFormatter = exceptionFormatter;
		this.requestQueue = new PipelineRequestQueue<T>(this);
		this.responseQueue = new PipelineResponseQueue<R>(this);
		this.channel = (SocketChannel) selectionKey.channel();
		this.requestFramer = new RequestFramer<T>(this, container);
		this.responseWriter = new ResponseWriter<R>(this, container);
		this.requestProcessor = new RequestProcessor<T, R>(this);
	}
	
	public void registerWriteInterest() {
		server.setWriteInterest(selectionKey, true);
	}
	
	public void unregisterWriteInterest() {
		server.setWriteInterest(selectionKey, false);
	}
	
	public void read() {
		lastRead = new Date();
		if (futureRead == null || futureRead.isDone()) {
			synchronized(this) {
				if (futureRead == null || futureRead.isDone()) {
					futureRead = server.submitIOTask(requestFramer);
				}
			}
		}
	}
	
	public void write() {
		lastWritten = new Date();
		if (futureWrite == null || futureWrite.isDone()) {
			synchronized(this) {
				if (futureWrite == null || futureWrite.isDone()) {
					futureWrite = server.submitIOTask(responseWriter);
				}
			}
		}
	}
	
	public void process() {
		lastProcessed = new Date();
		if (futureProcess == null || futureProcess.isDone()) {
			synchronized(this) {
				if (futureProcess == null || futureProcess.isDone()) {
					futureProcess = server.submitProcessTask(requestProcessor);
				}
			}
		}
	}

	/**
	 * If you simply "close" the channel only the input and output processing stops, not the message processing
	 * This method also stops the message processing
	 */
	public void die() {
		if (futureProcess != null && !futureProcess.isDone()) {
			futureProcess.cancel(true);
		}
		close();
	}
	
	@Override
	public void close() {
		try {
			closed = true;
			try {
				container.close();
			}
			catch (IOException e) {
				logger.debug("Failed to close the container", e);
			}
		
			if (futureRead != null && !futureRead.isDone()) {
				futureRead.cancel(true);
			}
			if (futureWrite != null && !futureWrite.isDone()) {
				futureWrite.cancel(true);
			}
		}
		finally {
			// remove it from the server map
			server.close(selectionKey);
		}
	}

	@Override
	public SecurityContext getSecurityContext() {
		return new SecurityContext() {
			@Override
			public SSLServerMode getSSLServerMode() {
				return server.getSSLServerMode();
			}
			@Override
			public SSLContext getSSLContext() {
				return server.getSSLContext();
			}
			@Override
			public Certificate[] getPeerCertificates() {
				return sslContainer == null ? null : sslContainer.getPeerCertificates();
			}
		};
	}

	@Override
	public Queue<T> getRequestQueue() {
		return requestQueue;
	}

	@Override
	public Queue<R> getResponseQueue() {
		return responseQueue;
	}

	SocketChannel getChannel() {
		return channel;
	}

	@Override
	public MessageFormatterFactory<R> getResponseFormatterFactory() {
		return responseFormatterFactory;
	}
	
	@Override
	public MessageParserFactory<T> getRequestParserFactory() {
		return requestParserFactory;
	}
	
	@Override
	public MessageProcessorFactory<T, R> getMessageProcessorFactory() {
		return messageProcessorFactory;
	}
	
	@Override
	public KeepAliveDecider<R> getKeepAliveDecider() {
		return keepAliveDecider;
	}
	
	@Override
	public ExceptionFormatter<T, R> getExceptionFormatter() {
		return exceptionFormatter;
	}
	
	public boolean isClosed() {
		return closed;
	}
	@Override
	public NIOServer getServer() {
		return server;
	}

	@Override
	public SourceContext getSourceContext() {
		return new SourceContext() {
			@Override
			public Socket getSocket() {
				return getChannel().socket();
			}
			@Override
			public Date getCreated() {
				return created;
			}
		};
	}

	@Override
	public PipelineState getState() {
		// check if we have been explicitly closed
		if (closed) {
			return PipelineState.CLOSED;
		}
		// otherwise check if we are no longer actively registered on the server
		else if (!server.getPipelines().contains(this)) {
			// if we have no messages that are being processed, no messages left on the response queue and nothing left to be done by the response writer, we are closed
			if ((futureProcess == null || futureProcess.isDone()) && responseQueue.isEmpty() && responseWriter.isDone()) {
				closed = true;
				return PipelineState.CLOSED;
			}
			else {
				return PipelineState.DRAINING;
			}
		}
		// if we have no data pending, we are waiting for more
		else if (requestQueue.isEmpty() && responseQueue.isEmpty() && responseWriter.isDone()) {
			return PipelineState.WAITING;
		}
		// we are processing stuff!
		else {
			return PipelineState.RUNNING;
		}
	}

	@Override
	public <Q, S> MessagePipeline<Q, S> upgrade(MessageParserFactory<Q> requestParserFactory, MessageFormatterFactory<S> responseFormatterFactory, MessageProcessorFactory<Q, S> messageProcessorFactory, KeepAliveDecider<S> keepAliveDecider, ExceptionFormatter<Q, S> exceptionFormatter) {
		MessagePipelineImpl<Q, S> pipeline = new MessagePipelineImpl<Q, S>(this, requestParserFactory, responseFormatterFactory, messageProcessorFactory, keepAliveDecider, exceptionFormatter);
		server.upgrade(selectionKey, pipeline);
		server.getDispatcher().fire(new ConnectionEventImpl(server, pipeline, ConnectionEvent.ConnectionState.UPGRADED), server);
		return pipeline;
	}

	MessagePipelineImpl<?, ?> getParentPipeline() {
		return parentPipeline;
	}

	ResponseWriter<R> getResponseWriter() {
		return responseWriter;
	}

	@Override
	public long getReadTimeout() {
		return readTimeout;
	}
	public void setReadTimeout(long readTimeout) {
		this.readTimeout = readTimeout;
	}

	@Override
	public long getWriteTimeout() {
		return writeTimeout;
	}
	public void setWriteTimeout(long writeTimeout) {
		this.writeTimeout = writeTimeout;
	}

	@Override
	public int getRequestLimit() {
		return requestLimit;
	}
	public void setRequestLimit(int requestLimit) {
		this.requestLimit = requestLimit;
	}

	@Override
	public int getResponseLimit() {
		return responseLimit;
	}
	public void setResponseLimit(int responseLimit) {
		this.responseLimit = responseLimit;
	}

	public Date getLastRead() {
		return lastRead;
	}
	public Date getLastWritten() {
		return lastWritten;
	}
	public Date getLastProcessed() {
		return lastProcessed;
	}
}
