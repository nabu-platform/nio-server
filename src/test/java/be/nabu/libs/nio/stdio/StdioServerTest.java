package be.nabu.libs.nio.stdio;

import java.io.IOException;
import java.util.concurrent.Executors;

import javax.net.ssl.SSLContext;

import be.nabu.libs.events.api.EventDispatcher;
import be.nabu.libs.events.api.EventHandler;
import be.nabu.libs.events.api.EventSubscription;
import be.nabu.libs.events.api.ResponseHandler;
import be.nabu.libs.nio.impl.NIOServerImpl;
import be.nabu.utils.io.SSLServerMode;

public class StdioServerTest {
	public static void main(String...args) throws IOException {
		SSLContext context = null;
		int port = 2099;
		int ioPoolSize = 3;
		int processPoolSize = 5;
		SSLServerMode sslServerMode = SSLServerMode.NO_CLIENT_CERTIFICATES;
		
		NIOServerImpl server = new NIOServerImpl(
			context, 
			sslServerMode,
			port, 
			ioPoolSize, 
			processPoolSize, 
			new StdioPipelineFactory(), 
			new EventDispatcher() {
				@Override
				public <E> void fire(E event, Object source) {
					System.out.println("Fire: " + event);
				}
				@Override
				public <E, R> R fire(E event, Object source, ResponseHandler<E, R> responseHandler) {
					System.out.println("Fire (with response): " + event);
					return (R) "test";
				}
				@Override
				public <E, R> R fire(E event, Object source, ResponseHandler<E, R> responseHandler, ResponseHandler<E, E> rewriteHandler) {
					System.out.println("Fire (with rewrite): " + event);
					return (R) "test";
				}
				@Override
				public <E, R> EventSubscription<E, R> subscribe(Class<E> eventType, EventHandler<E, R> eventHandler, Object... sources) {
					return null;
				}
				@Override
				public <E> EventSubscription<E, Boolean> filter(Class<E> eventType, EventHandler<E, Boolean> filter, Object... sources) {
					return null;
				}
			}, 
			Executors.defaultThreadFactory()
		);
		server.start();
	}
}
