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
package org.jetel.util;


/**
 * A bounded container that is capable of load balancing tasks among a (limited)
 * number of TaskQueue-s (limit means number of simultaneously running tasks... there is also a limit on
 * the number of tasks in single queue - see maxQueueLength)
 * 
 * @author pnajvar
 * @since 2008-12-01
 * 
 */
public class TaskQueueManager {

	public static final int DEFAULT_CONCURRENT_LIMIT = 10;
	
	/*
	 * number of parallel running queues
	 * can be set only at construct time
	 */
	int concurrentLimit;
	/*
	 * maximum length of single queue
	 * when scheduling a task, either new queue is started with the task
	 * or the shortest queue is picked and added a task. Even in this situation, caller
	 * might get blocked until the queue finishes previous tasks in the queue
	 * can be set only at construct time
	 */
	int maxQueueLength;
	
	/*
	 * A pool of task queues... has limited size specified at construct time
	 */
	TaskQueue[] pool;
	/*
	 * Current number of running queues in pool
	 * if (currentPoolSize < concurrentLimit) then a new queue is started for additional task
	 * if (currentPoolSize == concurrentLimit) then additional task is scheduled in the shortest queue
	 * pool shrinks only if cancel() or finish() are issued
	 */
	int currentPoolSize = 0;
	
	/*
	 * output to console how tasks are being scheduled
	 */
	boolean verbose = false;;
	
	public boolean isVerbose() {
		return verbose;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	public TaskQueueManager() {
		this(DEFAULT_CONCURRENT_LIMIT, TaskQueue.DEFAULT_MAX_QUEUE_LENGTH);
	}
	
	public TaskQueueManager(int concurrentLimit) {
		this(concurrentLimit, TaskQueue.DEFAULT_MAX_QUEUE_LENGTH);
	}
	
	public TaskQueueManager(int concurrentLimit, int maxQueueLength) {
		this.concurrentLimit = concurrentLimit;
		this.maxQueueLength = maxQueueLength;
		this.pool = new TaskQueue[concurrentLimit];
	}
	
	/**
	 * Schedules a task
	 * Either in new queue (if pool size is less than maximum)
	 * or in some existing queue if pool reached its maximum
	 * @param task
	 * @return Returns true if task has been successfully scheduled or false if it has been ignored
	 */
	public synchronized boolean addTask(Runnable task) {
		if (task == null) {
			trace("null task ignored");
			return false;
		}
		if (currentPoolSize < concurrentLimit) {
			this.currentPoolSize++;
			scheduleNewQueue(this.currentPoolSize-1, task);
			return true;
		} else {
			// lets find the best suitable task queue
			int pickedIndex = -1;
			int pickedSize = Integer.MAX_VALUE;
			for(int i = 0; i < currentPoolSize; i++) {
				if (! pool[i].isAlive()) {
					// dead queue can be replaced
					scheduleNewQueue(i, task);
					return true; // well, ok... this is not the best practise, isn't it?
				}
				if (pool[i].size() < pickedSize) {
					pickedIndex = i;
					pickedSize = pool[i].size();
				}
			}
			if (pickedIndex >= 0) {
				trace("Scheduling task " + task + " to queue [" + pickedIndex + "]");
				pool[pickedIndex].addTask(task);
				return true;
			} else {
				trace("Cannot schedule task " + task + " - pool size too small?");
				return false;
			}
		}
	}
	
	/**
	 * finish all tasks in all queues and wait for it
	 * This method blocks until all tasks in all queues are executed
	 * As a result all queues are marked as dead
	 * No need to worry though, because subsequent call to addTask() will start new queues silently
	 */
	public synchronized void finish() {
		trace("Finishing up to " + currentPoolSize + " queues");
		int cnt = 0;
		for(int i = 0; i < currentPoolSize; i++) {
			if (pool[i].isAlive()) {
				cnt++;
				trace("Finishing queue [" + i + "]");
				pool[i].finish();
				trace("Finishing queue [" + i + "] -- finished");
			}
		}
		trace("Finished " + cnt + " queues");
	}
	
	
	void scheduleNewQueue(int poolIndex, Runnable task) {
		trace("Starting new queue [" + poolIndex + "] with one task " + task);
		this.pool[poolIndex] = new TaskQueue(maxQueueLength);
		this.pool[poolIndex].addTask(task);
		this.pool[poolIndex].start();
	}
	
	public int getConcurrentLimit() {
		return concurrentLimit;
	}

	public void setConcurrentLimit(int concurrentLimit) {
		this.concurrentLimit = concurrentLimit;
	}

	void trace(String message) {
		if (verbose) {
			System.out.println("TaskQueueManager: " + message);
		}
	}
	
}
