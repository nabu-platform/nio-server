package be.nabu.libs.nio.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.libs.events.api.EventDispatcher;
import be.nabu.libs.metrics.api.MetricGauge;
import be.nabu.libs.metrics.api.MetricInstance;
import be.nabu.libs.nio.api.ConnectionAcceptor;
import be.nabu.libs.nio.api.NIOServer;
import be.nabu.libs.nio.api.Pipeline;
import be.nabu.libs.nio.api.PipelineFactory;
import be.nabu.libs.nio.api.events.ConnectionEvent;
import be.nabu.libs.nio.impl.events.ConnectionEventImpl;
import be.nabu.utils.io.SSLServerMode;

/**
 * TODO: 
 * - track start of read (for a single request) with a timestamp, if read takes too long (especially in the synchronous part but also async) > kill connection
 * - same for start of write + timeout
 * - set limits on the size of the incoming/outgoing queues, not entirely sure if browsers have preconfigured limits on how many http requests they pipeline (https://en.wikipedia.org/wiki/HTTP_pipelining)
 * 		for firefox this is configured in "network.http.pipelining.maxrequests" which defaults to 32 but "network.http.pipelining" is disabled by default?
 */
public class NIOServerImpl implements NIOServer {
	
	public static String METRIC_ACCEPTED_CONNECTIONS = "acceptedConnections";
	public static String METRIC_REJECTED_CONNECTIONS = "rejectedConnections";
	public static String METRIC_CURRENT_CONNECTIONS = "currentConnections";
	public static String METRIC_ACTIVE_IO_THREADS = "activeIOThreads";
	public static String METRIC_ACTIVE_PROCESS_THREADS = "activeProcessThreads";
	public static String METRIC_IDLE_IO_THREADS = "idleIOThreads";
	public static String METRIC_IDLE_PROCESS_THREADS = "idleProcessThreads";
	
	private static String CHANNEL_TYPE_CLIENT = "client";
    private static String CHANNEL_TYPE_SERVER = "server";
    private static String CHANNEL_TYPE = "channelType";
    
	private ServerSocketChannel channel;
	private int port;
	private Selector selector;
	private Map<SocketChannel, Pipeline> channels = new LinkedHashMap<SocketChannel, Pipeline>();
	private SSLContext sslContext;
	private MetricInstance metrics;
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private ExecutorService ioExecutors, processExecutors;
	private SSLServerMode sslServerMode;
	private PipelineFactory pipelineFactory;
	private ConnectionAcceptor connectionAcceptor;
	private EventDispatcher dispatcher;
	
	public NIOServerImpl(SSLContext sslContext, SSLServerMode sslServerMode, int port, int ioPoolSize, int processPoolSize, PipelineFactory pipelineFactory, EventDispatcher dispatcher, ThreadFactory threadFactory) {
		this.sslContext = sslContext;
		this.sslServerMode = sslServerMode;
		this.port = port;
		this.pipelineFactory = pipelineFactory;
		this.dispatcher = dispatcher;
		this.ioExecutors = Executors.newFixedThreadPool(ioPoolSize, threadFactory);
		this.processExecutors = Executors.newFixedThreadPool(processPoolSize, threadFactory);
	}
	
	public Future<?> submitIOTask(Runnable runnable) {
		return ioExecutors.submit(runnable);
	}
	public Future<?> submitProcessTask(Runnable runnable) {
		return processExecutors.submit(runnable);
	}
	
	@Override
	public void close(SelectionKey selectionKey) {
		try {
			dispatcher.fire(new ConnectionEventImpl(this, channels.get(selectionKey.channel()), ConnectionEvent.ConnectionState.CLOSED), this);
			if (selectionKey != null) {
				selectionKey.cancel();
			}
			if (channels.containsKey(selectionKey.channel())) {
				synchronized(channels) {
					channels.remove(selectionKey.channel());
				}
			}
		}
		finally {
			try {
				selectionKey.channel().close();
			}
			catch (IOException e) {
				logger.error("Failed to close the channel", e);
			}
		}
	}
	
	public static String getUserId(Socket socket) {
		InetSocketAddress remoteSocketAddress = ((InetSocketAddress) socket.getRemoteSocketAddress());
		return remoteSocketAddress == null ? "unknown:" + socket.getPort() : remoteSocketAddress.getAddress().getHostAddress() + ":" + socket.getPort();
	}
	
	@SuppressWarnings("unchecked")
	public void start() throws IOException {
		channel = ServerSocketChannel.open();
		channel.bind(new InetSocketAddress(port));		// new InetSocketAddress("localhost", port)
		channel.configureBlocking(false);
		// http://www.unixguide.net/network/socketfaq/4.5.shtml
		// after releasing the socket on close() it may remain open on the system
		// this will allow us to quickly stop and restart the server, otherwise we run the risk that we get an "Address already in use" exception
		channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
		
		selector = Selector.open();
		
		// we're only interested in new (accepted) connections
		SelectionKey socketServerSelectionKey = channel.register(selector, SelectionKey.OP_ACCEPT);
		
		Map<String, String> properties = new HashMap<String, String>();
        properties.put(CHANNEL_TYPE, CHANNEL_TYPE_SERVER);
        socketServerSelectionKey.attach(properties);
        
        while(true) {
        	// the selectNow() does NOT block whereas the select() does
        	// we want to make sure we update the interestOps() as soon as possible, we can't do a fully blocking wait here, unregistered write ops would simply be ignored
        	// the selectNow() however ends up in a permanent while loop and takes up 100% of at least one thread
        	// luckily the selector provides a wakeup() which unblocks the select() from another thread, this combines low overhead with quick interestops() updates
        	if (selector.select() == 0) {
        		continue;
        	}
        	Set<SelectionKey> selectedKeys = selector.selectedKeys();
        	Iterator<SelectionKey> iterator = selectedKeys.iterator();
        	while(iterator.hasNext()) {
        		SelectionKey key = iterator.next();
        		try {
	        		try {
		        		if (CHANNEL_TYPE_SERVER.equals(((Map<String, String>) key.attachment()).get(CHANNEL_TYPE))) {
		        			ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
		        			SocketChannel clientSocketChannel = serverSocketChannel.accept();
		        			MetricInstance metrics = this.metrics;
		        			if (clientSocketChannel != null) {
		        				if (connectionAcceptor != null && !connectionAcceptor.accept(this, clientSocketChannel)) {
		        					logger.warn("Connection rejected: " + clientSocketChannel.socket());
		        					if (metrics != null) {
										metrics.increment(METRIC_REJECTED_CONNECTIONS + ":" + getUserId(clientSocketChannel.socket()), 1l);
									}
		        					dispatcher.fire(new ConnectionEventImpl(this, null, ConnectionEvent.ConnectionState.REJECTED), this);
		        					clientSocketChannel.close();
		        				}
		        				else {
			        				clientSocketChannel.configureBlocking(false);
			        				
			        				// initially we are only interested in available data from the remote user
			        				SelectionKey clientKey = clientSocketChannel.register(selector, SelectionKey.OP_READ);
			        				
			        				// set a key we can detect
			        				Map<String, String> clientproperties = new HashMap<String, String>();
			                        clientproperties.put(CHANNEL_TYPE, CHANNEL_TYPE_CLIENT);
			                        clientKey.attach(clientproperties);
			                        
			                        if (!channels.containsKey(clientSocketChannel)) {
			                        	try {
					                        synchronized(channels) {
				                        		if (!channels.containsKey(clientSocketChannel)) {
					                        		logger.debug("New connection: {}", clientSocketChannel);
													Pipeline newPipeline = pipelineFactory.newPipeline(this, clientKey);
													channels.put(clientSocketChannel, newPipeline);
													if (metrics != null) {
														metrics.increment(METRIC_ACCEPTED_CONNECTIONS + ":" + getUserId(clientSocketChannel.socket()), 1l);
													}
													dispatcher.fire(new ConnectionEventImpl(this, newPipeline, ConnectionEvent.ConnectionState.CONNECTED), this);
					                        	}
					                        }
			                        	}
			                        	catch (IOException e) {
			                        		logger.error("Failed pipeline", e);
			                        		clientSocketChannel.close();
			                        	}
			                        }
		        				}
		        			}
		        		}
		        		else {
		        			SocketChannel clientChannel = (SocketChannel) key.channel();
		        			if (!channels.containsKey(clientChannel)) {
		        				logger.warn("No channel, cancelling key for: {}", clientChannel.socket());
		        				close(key);
		        			}
		        			else if (!clientChannel.isConnected() || !clientChannel.isOpen() || clientChannel.socket().isInputShutdown()) {
		        				logger.warn("Disconnected, cancelling key for: {}", clientChannel.socket());
		        				Pipeline pipeline = channels.get(clientChannel);
		        				if (pipeline != null) {
									pipeline.close();
		        				}
		        				else {
		        					close(key);
		        				}
		        			}
		        			else {
		        				if (key.isReadable() && channels.containsKey(clientChannel)) {
		        					Pipeline pipeline = channels.get(clientChannel);
			        				if (pipeline != null) {
			        					logger.trace("Scheduling pipeline, new data for: {}", clientChannel.socket());
			        					pipeline.read();
			        				}
		        				}
			        			if (key.isWritable() && channels.containsKey(clientChannel)) {
			        				logger.trace("Scheduling write processor, write buffer available for: {}", clientChannel.socket());
			        				Pipeline pipeline = channels.get(clientChannel);
			        				if (pipeline != null) {
			        					pipeline.write();
			        				}
			        			}
		        			}
		        		}
	        		}
	        		catch(CancelledKeyException e) {
	        			Pipeline pipeline = channels.get(key.channel());
	        			if (pipeline != null) {
		        			pipeline.close();
	        			}
	        			else {
	        				close(key);
	        			}
	        		}
        		}
        		catch (Exception e) {
        			e.printStackTrace();
        		}
        		finally {
        			iterator.remove();
        		}
        	}
        }
	}

	public SSLContext getSSLContext() {
		return sslContext;
	}

	public SSLServerMode getSSLServerMode() {
		return sslServerMode == null ? SSLServerMode.NO_CLIENT_CERTIFICATES : sslServerMode;
	}

	@Override
	public void setWriteInterest(SelectionKey selectionKey, boolean isInterested) {
		if (isInterested) {
			selectionKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
		}
		else {
			selectionKey.interestOps(SelectionKey.OP_READ);
		}
		if (selector != null) {
			selector.wakeup();
		}		
	}
	
	@Override
	public void stop() {
		if (channel != null) {
			try {
				channel.close();
				for (Pipeline pipeline : channels.values()) {
					try {
						pipeline.close();
					}
					catch (Exception e) {
						logger.error("Could not close pipeline", e);
					}
				}
				synchronized(channels) {
					channels.clear();
				}
				channel = null;
			}
			catch (IOException e) {
				logger.error("Failed to close server", e);
			}
		}
	}
	
	@Override
	protected void finalize() {
		stop();
	}

	@Override
	public PipelineFactory getPipelineFactory() {
		return pipelineFactory;
	}

	@Override
	public ConnectionAcceptor getConnectionAcceptor() {
		return connectionAcceptor;
	}

	@Override
	public void setConnectionAcceptor(ConnectionAcceptor connectionAcceptor) {
		this.connectionAcceptor = connectionAcceptor;
	}

	@Override
	public Collection<Pipeline> getPipelines() {
		return channels.values();
	}

	@Override
	public void upgrade(SelectionKey key, Pipeline pipeline) {
		if (channels.containsKey(key.channel())) {
			synchronized(channels) {
				if (channels.containsKey(key.channel())) {
					channels.put((SocketChannel) key.channel(), pipeline);
				}
			}
		}
	}

	@Override
	public EventDispatcher getDispatcher() {
		return dispatcher;
	}

	@Override
	public MetricInstance getMetrics() {
		return metrics;
	}

	@Override
	public void setMetrics(MetricInstance metrics) {
		if (metrics != null) {
			metrics.set(METRIC_CURRENT_CONNECTIONS, new MetricGauge() {
				@Override
				public long getValue() {
					return channels.size();
				}
			});
			if (ioExecutors instanceof ThreadPoolExecutor) {
				metrics.set(METRIC_ACTIVE_IO_THREADS, new MetricGauge() {
					@Override
					public long getValue() {
						return ((ThreadPoolExecutor) ioExecutors).getActiveCount();
					}
				});
				metrics.set(METRIC_IDLE_IO_THREADS, new MetricGauge() {
					@Override
					public long getValue() {
						return ((ThreadPoolExecutor) ioExecutors).getMaximumPoolSize() - ((ThreadPoolExecutor) ioExecutors).getActiveCount();
					}
				});
			}
			if (processExecutors instanceof ThreadPoolExecutor) {
				metrics.set(METRIC_ACTIVE_PROCESS_THREADS, new MetricGauge() {
					@Override
					public long getValue() {
						return ((ThreadPoolExecutor) processExecutors).getActiveCount();
					}
				});
				metrics.set(METRIC_IDLE_PROCESS_THREADS, new MetricGauge() {
					@Override
					public long getValue() {
						return ((ThreadPoolExecutor) processExecutors).getMaximumPoolSize() - ((ThreadPoolExecutor) processExecutors).getActiveCount();
					}
				});
			}
		}
		this.metrics = metrics;
	}
	
}
