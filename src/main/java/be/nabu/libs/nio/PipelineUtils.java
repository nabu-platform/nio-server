/*
* Copyright (C) 2015 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

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
