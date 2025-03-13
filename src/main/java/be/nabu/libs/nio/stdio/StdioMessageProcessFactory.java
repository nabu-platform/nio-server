package be.nabu.libs.nio.stdio;

import be.nabu.libs.events.api.EventDispatcher;
import be.nabu.libs.nio.api.ExceptionFormatter;
import be.nabu.libs.nio.api.MessageProcessor;
import be.nabu.libs.nio.api.MessageProcessorFactory;
import be.nabu.libs.nio.impl.EventDrivenMessageProcessor;

public class StdioMessageProcessFactory implements MessageProcessorFactory<String, String> {

	private EventDispatcher dispatcher;
	private ExceptionFormatter<String, String> exceptionFormatter;
	
	public StdioMessageProcessFactory(EventDispatcher dispatcher, ExceptionFormatter<String, String> exceptionFormatter) {
		this.dispatcher = dispatcher;
		this.exceptionFormatter = exceptionFormatter;
	}
	
	@Override
	public MessageProcessor<String, String> newProcessor(String request) {
		return new EventDrivenMessageProcessor<String, String>(String.class, String.class, dispatcher, exceptionFormatter, false);
	}

}
