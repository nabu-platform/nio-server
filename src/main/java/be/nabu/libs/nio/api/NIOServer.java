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

package be.nabu.libs.nio.api;

import java.nio.channels.SelectionKey;
import java.util.Collection;
import java.util.concurrent.Future;

import be.nabu.libs.events.api.EventTarget;

/**
 * The server will dispatch events concerning new connections and closed connections over the event dispatcher
 */
public interface NIOServer extends Server, EventTarget {
	public Future<?> submitIOTask(Runnable runnable);
	public Future<?> submitProcessTask(Runnable runnable);
	public void close(SelectionKey key);
	public void setWriteInterest(SelectionKey key, boolean isInterested);
	public void setReadInterest(SelectionKey key, boolean isInterested);
	public void upgrade(SelectionKey key, Pipeline pipeline);
	public PipelineFactory getPipelineFactory();
	public Collection<Pipeline> getPipelines();
	public ConnectionAcceptor getConnectionAcceptor();
	public void setConnectionAcceptor(ConnectionAcceptor connectionAcceptor);
	public NIODebugger getDebugger();
	public boolean isStopping();
	public boolean isRunning();
}
