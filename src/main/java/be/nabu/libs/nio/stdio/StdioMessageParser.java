package be.nabu.libs.nio.stdio;

import java.io.IOException;
import java.text.ParseException;

import be.nabu.libs.nio.api.MessageParser;
import be.nabu.libs.nio.api.MessagePipeline;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.CharBuffer;
import be.nabu.utils.io.api.DelimitedCharContainer;
import be.nabu.utils.io.api.PushbackContainer;
import be.nabu.utils.io.api.ReadableContainer;
import be.nabu.utils.io.buffers.bytes.ByteBufferFactory;
import be.nabu.utils.io.buffers.bytes.DynamicByteBuffer;
import be.nabu.utils.io.containers.chars.ReadableStraightByteToCharContainer;

public class StdioMessageParser implements MessageParser<String> {

	private DynamicByteBuffer initialBuffer;
	private StdioMessageParserFactory factory;
	private MessagePipeline<String, String> pipeline;
	private boolean isClosed, done, identified = true;
	private String message;

	public StdioMessageParser(StdioMessageParserFactory factory, MessagePipeline<String, String> pipeline) {
		this.factory = factory;
		this.pipeline = pipeline;
		initialBuffer = new DynamicByteBuffer();
		initialBuffer.mark();
	}
	
	@Override
	public void close() throws IOException {
		pipeline.close();
	}

	@Override
	public void push(PushbackContainer<ByteBuffer> content) throws ParseException, IOException {
		initialBuffer.reset();
		ByteBuffer limitedBuffer = ByteBufferFactory.getInstance().limit(initialBuffer, null, factory.getValidator().getMaxMessageLength() - initialBuffer.remainingData());
		long read = content.read(limitedBuffer);
		isClosed |= read == -1;
		ReadableContainer<CharBuffer> data = new ReadableStraightByteToCharContainer(initialBuffer);
		DelimitedCharContainer delimit = IOUtils.delimit(data, "\n");
		String request = IOUtils.toString(delimit);
		if (!delimit.isDelimiterFound()) {
			if (request.length() >= factory.getValidator().getMaxMessageLength()) {
				throw new IllegalArgumentException("Message too large");
			}
		}
		else {
			initialBuffer.remark();
			message = request;
			done = true;
		}
		if (isDone() && initialBuffer.remainingData() > 0) {
			content.pushback(initialBuffer);
		}		
	}

	@Override
	public boolean isIdentified() {
		return identified;
	}

	@Override
	public boolean isDone() {
		return done;
	}

	@Override
	public boolean isClosed() {
		return isClosed;
	}

	@Override
	public String getMessage() {
		return message;
	}

}
