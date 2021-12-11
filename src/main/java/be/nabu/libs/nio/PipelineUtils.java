package be.nabu.libs.nio;

import java.util.UUID;

import be.nabu.libs.nio.api.Pipeline;

public class PipelineUtils {
	
	private static ThreadLocal<Pipeline> pipeline = new ThreadLocal<Pipeline>();
	
	public static void setPipelineForThread(Pipeline pipeline) {
		PipelineUtils.pipeline.set(pipeline);
	}
	
	public static Pipeline getPipeline() {
		return pipeline.get();
	}
	
	public static String getPipelineId(Pipeline pipeline) {
		Object object = pipeline.getContext().get("pipeline-id");
		if (object == null) {
			object = UUID.randomUUID().toString().replace("-", "");
			pipeline.getContext().put("pipeline-id", object);
		}
		return object.toString();
	}
	
	public static String getPipelineId() {
		Pipeline pipeline = getPipeline();
		return pipeline == null ? null : getPipelineId(pipeline);
	}
}
