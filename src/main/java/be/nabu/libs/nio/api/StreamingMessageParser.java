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

/**
 * A streaming message parser will parse something initially, then create a result prematurely
 * This result is already sent back to somewhere which is assumed to continue reading from the incoming stream of bytes
 * Only once the stream is fully read do we consider the message fully handled
 */
public interface StreamingMessageParser<T> extends MessageParser<T> {
	public boolean isStreamed();
	// even a streaming parser can choose not to enable streaming mode
	public default boolean isStreaming() {
		return true;
	}
}
