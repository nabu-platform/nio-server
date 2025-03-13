package be.nabu.libs.nio.stdio;

import java.io.IOException;
import java.nio.channels.SelectionKey;

import be.nabu.libs.nio.api.ExceptionFormatter;
import be.nabu.libs.nio.api.KeepAliveDecider;
import be.nabu.libs.nio.api.MessageFormatter;
import be.nabu.libs.nio.api.MessageFormatterFactory;
import be.nabu.libs.nio.api.NIOServer;
import be.nabu.libs.nio.api.Pipeline;
import be.nabu.libs.nio.api.PipelineFactory;
import be.nabu.libs.nio.impl.MessagePipelineImpl;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.ReadableContainer;

public class StdioPipelineFactory implements PipelineFactory {

	@Override
	public Pipeline newPipeline(NIOServer server, SelectionKey key) throws IOException {
		StdioMessageParserFactory requestParserFactory = new StdioMessageParserFactory();
		
		ExceptionFormatter<String, String> exceptionFormatter = new ExceptionFormatter<String, String>() {
			@Override
			public String format(String request, Exception e) {
				System.out.println("FORMATTING EXCEPTION: " + e.getMessage());
				return "221 " + e.getMessage();
			}
		};
		
		MessagePipelineImpl<String, String> pipeline = new MessagePipelineImpl<String, String>(
			server,
			key,
			requestParserFactory,
			new MessageFormatterFactory<String>() {
				@Override
				public MessageFormatter<String> newMessageFormatter() {
					return new MessageFormatter<String>() {
						@Override
						public ReadableContainer<ByteBuffer> format(String message) {
							return IOUtils.wrap((message + "\n").getBytes(), true);
						}
					};
				}
			},
			new StdioMessageProcessFactory(server.getDispatcher(), exceptionFormatter),
			new KeepAliveDecider<String>() {
				@Override
				public boolean keepConnectionAlive(String response) {
					return response != null && !response.startsWith("221 ");
				}
			},
			exceptionFormatter
		);
		
		requestParserFactory.setPipeline(pipeline);
		
		return pipeline;
	}

}
