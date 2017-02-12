package be.nabu.libs.nio.impl;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.Container;
import be.nabu.utils.io.api.WritableContainer;
import be.nabu.utils.io.containers.ComposedContainer;
import be.nabu.utils.io.containers.ReadableContainerDuplicator;
import be.nabu.utils.io.containers.WritableContainerMulticaster;

public class ContainerDebugger {
	
	public static final class PrintContainer implements WritableContainer<ByteBuffer> {
		
		private Logger logger = LoggerFactory.getLogger(getClass());
		
		private String message;

		public PrintContainer() {
		}
		public PrintContainer(String message) {
			this.message = message;
		}
		
		@Override
		public void close() throws IOException {
		}

		@Override
		public long write(ByteBuffer buffer) throws IOException {
			long remainingData = buffer.remainingData();
			String message = new String(IOUtils.toBytes(buffer));
			if (this.message == null) {
				logger.info(message);
			}
			else {
				logger.info(this.message.replace("${message}", message));
			}
			return remainingData;
		}

		@Override
		public void flush() throws IOException {
		}
	}

	@SuppressWarnings("unchecked")
	public static Container<ByteBuffer> debug(Container<ByteBuffer> container) {
		PrintContainer printContainer = new PrintContainer();
		return new ComposedContainer<ByteBuffer>(
			new ReadableContainerDuplicator<ByteBuffer>(container, printContainer),
			new WritableContainerMulticaster<ByteBuffer>(container, printContainer)
		);
	}
}
