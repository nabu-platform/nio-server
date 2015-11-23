package be.nabu.libs.nio.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.libs.events.api.EventDispatcher;
import be.nabu.libs.nio.api.ConnectionAcceptor;
import be.nabu.libs.nio.api.Pipeline;
import be.nabu.libs.nio.api.PipelineFactory;
import be.nabu.libs.nio.api.NIOServer;
import be.nabu.libs.nio.api.events.ConnectionEvent;
import be.nabu.libs.nio.impl.events.ConnectionEventImpl;
import be.nabu.utils.io.SSLServerMode;

public class NIOServerImpl implements NIOServer {
	
	private static String CHANNEL_TYPE_CLIENT = "client";
    private static String CHANNEL_TYPE_SERVER = "server";
    private static String CHANNEL_TYPE = "channelType";
    
	private ServerSocketChannel channel;
	private int port;
	private Selector selector;
	private Map<SocketChannel, Pipeline> channels = new HashMap<SocketChannel, Pipeline>();
	private SSLContext sslContext;
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private ExecutorService ioExecutors, processExecutors;
	private SSLServerMode sslServerMode;
	private PipelineFactory pipelineFactory;
	private ConnectionAcceptor connectionAcceptor;
	private EventDispatcher dispatcher;
	
	public NIOServerImpl(SSLContext sslContext, SSLServerMode sslServerMode, int port, int ioPoolSize, int processPoolSize, PipelineFactory pipelineFactory, EventDispatcher dispatcher) {
		this.sslContext = sslContext;
		this.sslServerMode = sslServerMode;
		this.port = port;
		this.pipelineFactory = pipelineFactory;
		this.dispatcher = dispatcher;
		this.ioExecutors = Executors.newFixedThreadPool(ioPoolSize);
		this.processExecutors = Executors.newFixedThreadPool(processPoolSize);
	}
	
	public Future<?> submitIOTask(Runnable runnable) {
		return ioExecutors.submit(runnable);
	}
	public Future<?> submitProcessTask(Runnable runnable) {
		return processExecutors.submit(runnable);
	}
	
	@Override
	public void close(SelectionKey selectionKey) {
		dispatcher.fire(new ConnectionEventImpl(((SocketChannel) selectionKey.channel()).socket(), ConnectionEvent.ConnectionState.CLOSED), this);
		if (channels.containsKey(selectionKey.channel())) {
			synchronized(channels) {
				channels.remove(selectionKey.channel());
			}
		}
		if (selectionKey != null) {
			selectionKey.cancel();
			try {
				selectionKey.channel().close();
			}
			catch (IOException e) {
				logger.error("Failed to close the channel", e);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	public void start() throws IOException {
		channel = ServerSocketChannel.open();
		channel.bind(new InetSocketAddress(port));		// new InetSocketAddress("localhost", port)
		channel.configureBlocking(false);
		
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
		        			if (clientSocketChannel != null) {
		        				if (connectionAcceptor != null && !connectionAcceptor.accept(this, clientSocketChannel)) {
		        					logger.warn("Connection rejected: " + clientSocketChannel.socket());
		        					dispatcher.fire(new ConnectionEventImpl(clientSocketChannel.socket(), ConnectionEvent.ConnectionState.REJECTED), this);
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
				                        synchronized(channels) {
				                        	if (!channels.containsKey(clientSocketChannel)) {
					                        	try {
					                        		logger.debug("New connection: {}", clientSocketChannel);
													channels.put(clientSocketChannel, pipelineFactory.newPipeline(this, clientKey));
													dispatcher.fire(new ConnectionEventImpl(clientSocketChannel.socket(), ConnectionEvent.ConnectionState.CONNECTED), this);
					                        	}
					                        	catch (IOException e) {
					                        		logger.error("Failed pipeline", e);
					                        		clientSocketChannel.close();
					                        	}
				                        	}
				                        }
			                        }
		        				}
		        			}
		        		}
		        		else {
		        			SocketChannel clientChannel = (SocketChannel) key.channel();
		        			if (!channels.containsKey(clientChannel)) {
		        				logger.warn("No channel, cancelling key for: {}", clientChannel.socket());
		        				key.cancel();
		        			}
		        			else if (!clientChannel.isConnected() || !clientChannel.isOpen() || clientChannel.socket().isInputShutdown()) {
		        				logger.warn("Disconnected, cancelling key for: {}", clientChannel.socket());
		        				key.cancel();
		        				if (channels.containsKey(clientChannel)) {
			        				synchronized(channels) {
			        					if (channels.containsKey(clientChannel)) {
				        					channels.get(clientChannel).close();
				        					channels.remove(clientChannel);
			        					}
			        				}
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
		        			synchronized(channels) {
		        				channels.remove(key.channel());
		        			}
		        			pipeline.close();
	        			}
	        			key.channel().close();
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
				synchronized(channels) {
					for (Pipeline pipeline : channels.values()) {
						pipeline.close();
					}
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
}
