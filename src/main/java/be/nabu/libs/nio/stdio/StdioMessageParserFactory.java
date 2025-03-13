package be.nabu.libs.nio.stdio;

import be.nabu.libs.nio.api.MessageParser;
import be.nabu.libs.nio.api.MessageParserFactory;
import be.nabu.libs.nio.api.MessagePipeline;

public class StdioMessageParserFactory implements MessageParserFactory<String> {

	private StdioValidator stdioValidator;
	private MessagePipeline<String, String> pipeline;

	public StdioMessageParserFactory() {
		stdioValidator = new StdioValidator(1024l*1024l*10);
	}
	
	@Override
	public MessageParser<String> newMessageParser() {
		return new StdioMessageParser(this, pipeline);
	}

	public StdioValidator getValidator() {
		return stdioValidator;
	}
	
	public MessagePipeline<String, String> getPipeline() {
		return pipeline;
	}

	public void setPipeline(MessagePipeline<String, String> pipeline) {
		this.pipeline = pipeline;
	}

}
