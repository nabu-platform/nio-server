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

import java.util.Queue;

public interface MessagePipeline<T, R> extends Pipeline {
	/**
	 * A queue that contains all the pending requests
	 */
	public Queue<T> getRequestQueue();
	/**
	 * A queue that contains all the pending responses
	 */
	public Queue<R> getResponseQueue();
	/**
	 * The maximum size of the request queue
	 * A value of 0 means unlimited
	 */
	public int getRequestLimit();
	/**
	 * The maximum size of the response queue
	 * A value of 0 means unlimited
	 */
	public int getResponseLimit();
}
