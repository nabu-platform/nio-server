package be.nabu.libs.nio.api;

import java.io.IOException;
import java.nio.channels.SelectionKey;

public interface PipelineFactory {
	public Pipeline newPipeline(Server server, SelectionKey key) throws IOException;
}
