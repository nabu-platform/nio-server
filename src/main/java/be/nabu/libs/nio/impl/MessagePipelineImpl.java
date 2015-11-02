package be.nabu.libs.nio.impl;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.cert.Certificate;
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
import be.nabu.libs.nio.api.MessageProcessor;
import be.nabu.libs.nio.api.SecurityContext;
import be.nabu.libs.nio.api.NIOServer;
import be.nabu.libs.nio.api.SourceContext;
import be.nabu.utils.io.SSLServerMode;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.Container;
import be.nabu.utils.io.containers.bytes.SSLSocketByteContainer;
import be.nabu.utils.io.containers.bytes.SocketByteContainer;

public class MessagePipelineImpl<T, R> implements MessagePipeline<T, R> {
	
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
	private MessageProcessor<T, R> messageProcessor;
	private KeepAliveDecider<R> keepAliveDecider;
	private ExceptionFormatter<T, R> exceptionFormatter;
	
	private boolean closed;
	
	public MessagePipelineImpl(NIOServer server, SelectionKey selectionKey, MessageParserFactory<T> requestParserFactory, MessageFormatterFactory<R> responseFormatterFactory, MessageProcessor<T, R> messageProcessor, KeepAliveDecider<R> keepAliveDecider, ExceptionFormatter<T, R> exceptionFormatter) throws IOException {
		this.server = server;
		this.selectionKey = selectionKey;
		this.requestParserFactory = requestParserFactory;
		this.responseFormatterFactory = responseFormatterFactory;
		this.messageProcessor = messageProcessor;
		this.keepAliveDecider = keepAliveDecider;
		this.exceptionFormatter = exceptionFormatter;
		this.requestQueue = new PipelineRequestQueue<T>(this);
		this.responseQueue = new PipelineResponseQueue<R>(this);
		this.channel = (SocketChannel) selectionKey.channel();
		this.container = new SocketByteContainer(channel);
		// perform SSL if required
		if (server.getSSLContext() != null) {
			sslContainer = new SSLSocketByteContainer(container, server.getSSLContext(), server.getSSLServerMode());
			sslContainer.shakeHands();
			container = sslContainer;
		}
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
		if (futureRead == null || futureRead.isDone()) {
			synchronized(this) {
				if (futureRead == null || futureRead.isDone()) {
					futureRead = server.submitIOTask(requestFramer);
				}
			}
		}
	}
	
	public void write() {
		if (futureWrite == null || futureWrite.isDone()) {
			synchronized(this) {
				if (futureWrite == null || futureWrite.isDone()) {
					futureWrite = server.submitIOTask(responseWriter);
				}
			}
		}
	}
	
	public void process() {
		if (futureProcess == null || futureProcess.isDone()) {
			synchronized(this) {
				if (futureProcess == null || futureProcess.isDone()) {
					futureProcess = server.submitIOTask(requestProcessor);
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
		closed = true;
		try {
			container.close();
		}
		catch (IOException e) {
			logger.error("Failed to close the container", e);
		}
	
		if (futureRead != null && !futureRead.isDone()) {
			futureRead.cancel(true);
		}
		if (futureWrite != null && !futureWrite.isDone()) {
			futureWrite.cancel(true);
		}
		// remove it from the server map
		server.close(selectionKey);
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

	public MessageFormatterFactory<R> getResponseFormatterFactory() {
		return responseFormatterFactory;
	}
	public MessageParserFactory<T> getRequestParserFactory() {
		return requestParserFactory;
	}
	public MessageProcessor<T, R> getMessageProcessor() {
		return messageProcessor;
	}
	public KeepAliveDecider<R> getKeepAliveDecider() {
		return keepAliveDecider;
	}
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
		};
	}
	
}
