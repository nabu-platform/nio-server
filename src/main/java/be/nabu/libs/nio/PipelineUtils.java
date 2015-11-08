package be.nabu.libs.nio;

import be.nabu.libs.nio.api.Pipeline;

public class PipelineUtils {
	
	private static ThreadLocal<Pipeline> pipeline = new ThreadLocal<Pipeline>();
	
	public static void setPipelineForThread(Pipeline pipeline) {
		PipelineUtils.pipeline.set(pipeline);
	}
	
	public static Pipeline getPipeline() {
		return pipeline.get();
	}
}
