package org.jetel.util;

import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A simple task queue
 * 
 * Each task (actually a Runnable) is scheduled in the queue by
 * addTask(Runnable) method
 * 
 * Tasks are executed sequentially in single thread (the queue itself). This
 * thread waits until the queue is non-empty.
 * 
 * The size of the queue is limited by maxQueueLength. If it gets exceeded the thread calling addTask() is blocked until
 * the queue shrinks
 * 
 * @author Pavel
 * 
 */
public class TaskQueue extends Thread {

	public static final int DEFAULT_MAX_QUEUE_LENGTH = 100;
	
	private boolean terminated = false;
	private boolean finishing = false;
	private ArrayBlockingQueue<Runnable> taskQueue;
	private int maxQueueLength = DEFAULT_MAX_QUEUE_LENGTH;

	public TaskQueue() {
		this(DEFAULT_MAX_QUEUE_LENGTH);
	}
	
	public TaskQueue(int maxQueueLength) {
		taskQueue = new ArrayBlockingQueue<Runnable>(maxQueueLength);
	}
	
	public void addTask(Runnable task) {
		if (finishing) {
			// we will not schedule any more tasks since we are trying to finish
			return;
		}
		try {
			this.taskQueue.put(task);
		} catch (InterruptedException e) {
		}
	}

	public void run() {
		Runnable aTask = null;
		while (!terminated) {
			try {
				aTask = this.taskQueue.take();
				aTask.run();
			} catch (InterruptedException e) {
				terminated = true;
			}
		}
	}

	public int size() {
		return this.taskQueue.size();
	}

	public void cancel() {
		this.terminated = true;
	}

	/**
	 * 
	 */
	public void finish() {
		// we finish by adding a "last cancel" task and disabling all new tasks
		addTask(new Runnable() {
			public void run() {
				cancel();
			}
		});
		this.finishing = true;
		// ten we join current thread
		try {
			this.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
