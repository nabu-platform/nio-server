A generic non-blocking I/O server that supports both TCP and UDP.

The server supports a generic pipeline model where you can optionally go for message driven pipelines. There are currently request parsers for http and websockets available in other packages.
Note that the pipeline model works for both TCP and UDP allowing you to layer for example HTTP over UDP:

```java
public static void main(String...args) throws IOException, InterruptedException {
	EventDispatcherImpl dispatcher = new EventDispatcherImpl();

	HTTPPipelineFactoryImpl httpPipelineFactoryImpl = new HTTPPipelineFactoryImpl(
		new HTTPProcessorFactoryImpl(new DefaultHTTPExceptionFormatter(), false, dispatcher), 
		new MemoryMessageDataProvider()
	);
	
	final Date date = new Date();
	
	dispatcher.subscribe(HTTPRequest.class, new EventHandler<HTTPRequest, HTTPResponse>() {
		int counter = 0;
		@Override
		public HTTPResponse handle(HTTPRequest event) {
			System.out.println(++counter + " [" + (new Date().getTime() - date.getTime()) + " ms] Received request: " + event.getMethod() + " @ " + event.getTarget());
			return HTTPUtils.newEmptyResponse();
		}
	});
	
	final UDPServerImpl server = new UDPServerImpl(5000, 2, 5, httpPipelineFactoryImpl, dispatcher, Executors.defaultThreadFactory());
	Thread thread = new Thread(new Runnable() {
		@Override
		public void run() {
			try {
				server.start();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	});
	thread.setPriority(Thread.MAX_PRIORITY);
	thread.start();
	
	Thread.sleep(1000);
	InetSocketAddress remote = new InetSocketAddress("localhost", 5000);
	
	String content = "GET / HTTP/1.1\r\n"
		+ "Content-Length: 0\r\n"
		+ "Host: localhost\r\n\r\n";
	
	byte [] bytes = content.getBytes();
	
	int amount = 10000;
	DatagramChannel channel = DatagramChannel.open();
	// this saves on some overhead, if not connected the security manager is called more often
	channel.connect(remote);
	channel.configureBlocking(false);
	for (int i = 0; i < amount; i++) {
		channel.send(ByteBuffer.wrap(bytes), remote);
		if (i % 10 == 0) {
			Thread.sleep(0, 1);
		}
	}
	System.out.println("Sent " + amount + " in: " + (new Date().getTime() - date.getTime()) + "ms");
}
```