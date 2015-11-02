package be.nabu.libs.nio.api;

import java.io.IOException;
import java.nio.channels.SelectionKey;

public interface PipelineFactory {
	public Pipeline newPipeline(NIOServer server, SelectionKey key) throws IOException;
}
