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

import java.io.Closeable;
import java.io.IOException;
import java.text.ParseException;

import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.PushbackContainer;

/**
 * A message parser that can handle partial data pushes
 */
public interface MessageParser<T> extends Closeable {
	/**
	 * Push data to the framer
	 */
	public void push(PushbackContainer<ByteBuffer> content) throws ParseException, IOException;
	/**
	 * Check whether or not it is identified as a correct message
	 */
	public boolean isIdentified();
	/**
	 * Check whether or not the parsing is done
	 */
	public boolean isDone();
	/**
	 * Whether or not the data was considered closed, in this case the pipeline is shut down and no response can be sent back
	 * The request can still be processed though
	 */
	public boolean isClosed();
	/**
	 * Get the message (only works if isDone() returns true)
	 */
	public T getMessage();
}
