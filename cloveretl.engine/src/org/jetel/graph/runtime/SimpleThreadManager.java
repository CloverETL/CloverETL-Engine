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

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.jetel.graph.Result;

/**
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 27.2.2008
 */
public class SimpleThreadManager implements IThreadManager {

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IThreadManager#initWatchDog(org.jetel.graph.runtime.WatchDog)
	 */
	@Override
	public void initWatchDog(WatchDog watchDog) {
		watchDog.setThreadManager(this);
		watchDog.init();
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IThreadManager#executeWatchDog(org.jetel.graph.runtime.WatchDog)
	 */
	@Override
	public WatchDogFuture executeWatchDog(WatchDog watchDog) {
		WatchDogFutureImpl watchDogFuture = new WatchDogFutureImpl(watchDog); 
		Thread watchdogThread = new Thread(watchDogFuture, "WatchDog");
		watchdogThread.start();
		
		return watchDogFuture;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IThreadManager#executeNode(java.lang.Runnable)
	 */
	@Override
	public void executeNode(Runnable node) {
		Thread nodeThread = new Thread(node);
		nodeThread.setContextClassLoader(node.getClass().getClassLoader());
		nodeThread.setPriority(Thread.MIN_PRIORITY);
		nodeThread.setDaemon(false);
		nodeThread.start();
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IThreadManager#execute(java.lang.Runnable)
	 */
	@Override
	public void execute(Runnable runnable) {
		Thread nodeThread = new Thread(runnable.getClass().getName());
		nodeThread.setContextClassLoader(runnable.getClass().getClassLoader());
		nodeThread.setPriority(Thread.MIN_PRIORITY);
		nodeThread.setDaemon(false);
		nodeThread.start();
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IThreadManager#execute(java.lang.Runnable)
	 */
	@Override
	public <R extends Runnable> FutureOfRunnable<R> executeRunnable(R runnable) {
		FutureOfRunnableImpl<R> futureTask = new FutureOfRunnableImpl<R>(runnable);
		Thread thread = new Thread(futureTask, runnable.getClass().getName());
		thread.setContextClassLoader(runnable.getClass().getClassLoader());
		thread.setPriority(Thread.MIN_PRIORITY);
		thread.setDaemon(false);
		thread.start();
		
		return futureTask;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IThreadManager#execute(java.lang.Runnable)
	 */
	@Override
	public <C extends Callable<R>, R> FutureOfCallable<C, R> executeCallable(C callable) {
		FutureOfCallableImpl<C, R> futureTask = new FutureOfCallableImpl<C, R>(callable);
		Thread thread = new Thread(futureTask, callable.getClass().getName());
		thread.setContextClassLoader(callable.getClass().getClassLoader());
		thread.setPriority(Thread.MIN_PRIORITY);
		thread.setDaemon(false);
		thread.start();
		
		return futureTask;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IThreadManager#getFreeThreadsCount()
	 */
	@Override
	public int getFreeThreadsCount() {
		return Integer.MAX_VALUE;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IThreadManager#releaseNodeThreads(int)
	 */
	@Override
	public void releaseNodeThreads(int nodeThreadsToRelease) {
		// DO NOTHING
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IThreadManager#free()
	 */
	@Override
	public void free() {
		// DO NOTHING
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IThreadManager#freeNow()
	 */
	@Override
	public void freeNow() {
		// DO NOTHING
	}

	private class FutureOfCallableImpl<C extends Callable<R>, R> extends FutureTask<R> implements FutureOfCallable<C, R> {
		private C callable;
		
		public FutureOfCallableImpl(C callable) {
			super(callable);
			
			this.callable = callable;
		}

		@Override
		public C getCallable() {
			return callable;
		}
	}

	private class WatchDogFutureImpl extends FutureOfCallableImpl<WatchDog, Result> implements WatchDogFuture {
		public WatchDogFutureImpl(WatchDog watchDog) {
			super(watchDog);
		}

		@Override
		public WatchDog getWatchDog() {
			return getCallable();
		}
	}
	
	private class FutureOfRunnableImpl<R extends Runnable> extends FutureTask<Object> implements FutureOfRunnable<R> {
		private R runnable;
		
		public FutureOfRunnableImpl(R runnable) {
			super(runnable, new Object());
			
			this.runnable = runnable;
		}

		@Override
		public R getRunnable() {
			return runnable;
		}
	}

}
