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

import java.util.ArrayList;

import org.apache.log4j.MDC;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.ContextProvider.Context;
import org.jetel.graph.Node;

/**
 * This implementation of Runnable interface should be use for each thread inside a
 * component.
 * 
 * @see ContextProvider
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
 *
 * @created 24.9.2009
 */
public abstract class CloverWorker implements Runnable, Thread.UncaughtExceptionHandler {

	private Node node;
	
	private String name;
	
	private Thread thread;
	
	/**
	 * True if the worker is already running. This variable is guarded by 'this' monitor.
	 */
	private boolean isRunning;
	
	/**
	 * Similar to runIt in a component
	 * It tells the implementing class whether 
	 */
	protected boolean runIt;
	
	/**
	 * Exception thrown by user defined method {@link #work()}.
	 */
	protected Exception exception;
	
	private ArrayList<CloverWorkerListener> listeners = new ArrayList<CloverWorkerListener>();
	
	public CloverWorker(Node node, String name) {
		this.node = node;
		addCloverWorkerListener(node);
		this.name = name;
		this.isRunning = false;
		runIt = true;
	}
	
	@Override
	public void run() {
		Context c = ContextProvider.registerNode(node);
		try {
			MDC.put("runId", node.getGraph().getRuntimeContext().getRunId());
			//set a meaningful name of current thread
			thread = Thread.currentThread();
			Thread parentThread = node.getNodeThread();
			thread.setName((parentThread != null ? parentThread.getName() : node.getId()) + ": " + name);
			//register worker as a child thread of the parent component
			node.registerChildThread(thread);
			synchronized (this) {
				isRunning = true;
				notifyAll();
			}
			
			work();
			fireWorkerFinished(new CloverWorkerListener.Event(this));
		} catch (InterruptedException e) {
			// this is fine, we're not interested
		} catch (RuntimeException e) {
			exception = e;
			throw e;
		} catch (Exception e) {
			exception = e;
			throw new JetelRuntimeException(e);
		} finally {
			ContextProvider.unregister(c);
			node.unregisterChildThread(thread);
			MDC.remove("runId");
			Thread.currentThread().setName("<unnamed>");
		}
	}

	public synchronized void waitForStartup() {
		while (!isRunning) {
			try {
				wait();
			} catch (InterruptedException e) {
				throw new RuntimeException("Unexpected interruption of waiting for startup of worker.");
			}
		}
	}
	
	public Node getNode() {
		return node;
	}
	
	public abstract void work() throws Exception, InterruptedException;
	
	public Thread startWorker() {
		thread = new Thread(this);
		thread.setPriority(Thread.MIN_PRIORITY);
		thread.setDaemon(false);
		thread.setUncaughtExceptionHandler(this);
		thread.start();
		return thread;
	}

	/**
	 * Executes the given {@link CloverWorker} instance.
	 * @param cloverWorker
	 * @return future of the given {@link CloverWorker}
	 */
	private static IThreadManager localSimpleThreadManager = new SimpleThreadManager();
	public static <T extends CloverWorker> FutureOfRunnable<T> startWorker(T cloverWorker) {
		return cloverWorker.getNode().getGraph().getWatchDog().getThreadManager().executeRunnable(cloverWorker);
		
		//return localSimpleThreadManager.executeRunnable(cloverWorker);
	}
	
	public Thread getThread() {
		return thread;
	}
	
	public String getName() {
		return name;
	}
	
	public void addCloverWorkerListener(CloverWorkerListener l) {
		this.listeners.add(l);
	}
	
	public void removeCloverWorkerListener(CloverWorkerListener l) {
		this.listeners.remove(l);
	}
	
	void fireWorkerFinished(CloverWorkerListener.Event e) {
		for(CloverWorkerListener l : listeners) {
			l.workerFinished(e);
		}
	}
	
	void fireWorkerCrashed(CloverWorkerListener.Event e) {
		for(CloverWorkerListener l : listeners) {
			l.workerCrashed(e);
		}
	}
	
	/**
	 * @return exception thrown in {@link #work()} method if any
	 */
	public Exception getException() {
		return exception;
	}
	
	@Override
	public void uncaughtException(Thread t, Throwable e) {
		if (t == thread) {
			fireWorkerCrashed(new CloverWorkerListener.Event(this, e));
		}
	}
	
}