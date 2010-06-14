/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.graph.runtime;

import java.util.concurrent.Future;

import org.jetel.graph.Result;

/**
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 27.2.2008
 */
public interface IThreadManager {

	/**
	 * Executes given WatchDog.
	 * @param watchDog
	 * @return
	 */
	public Future<Result> executeWatchDog(WatchDog watchDog);
	
	/**
	 * Runs given runnable class via inner instance of executor service.
	 * It suspects that the given runnable instance is a node representation.
	 * @param runnable
	 */
	public void executeNode(Runnable node);
	
	/**
	 * Runs arbitrary runnable code with thread re-usability.
	 * @param node
	 */
	public void execute(Runnable runnable);
	
	/**
	 * Returns the approximate number of available free threads.
	 * @return number of threads
	 */
	public int getFreeThreadsCount();
	
	/**
	 * Decreases number of used threads.
	 * @param nodeThreadsToRelease
	 */
	public void releaseNodeThreads(int nodeThreadsToRelease);

	/**
	 * Waits for all currently running graphs are already done
	 * and finishes graph executor life cycle.
	 * New graphs cannot be submitted after free invocation.
	 */
	public void free();

	/**
	 * Immediately finishes graph executor life cycle. All running
	 * graphs are aborted.
	 */
	public void freeNow();

}
