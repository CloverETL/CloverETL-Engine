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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Jiri Musil (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jan 9, 2018
 */
public abstract class PoolingThreadManager implements IThreadManager {

	protected ThreadPoolExecutor watchdogExecutor;
	protected ThreadPoolExecutor nodeExecutor;
	protected ThreadPoolExecutor threadExecutor;
	
	@Override
	synchronized public void initWatchDog(WatchDog watchDog) {
		watchDog.setThreadManager(this);
		watchDog.init();
	}

	@Override
	public abstract WatchDogFuture executeWatchDog(WatchDog watchDog);

	@Override
	public abstract void executeNode(Runnable node);

	@Override
	public void execute(Runnable runnable) {
		threadExecutor.execute(runnable);
	}

	@Override
	public <T extends Runnable> FutureOfRunnable<T> executeRunnable(T runnable) {
		return new FutureOfRunnableWrapper<T>(threadExecutor.submit(runnable, new Object()), runnable);
	}

	@Override
	public <C extends Callable<R>, R> FutureOfCallable<C, R> executeCallable(C callable) {
		return new FutureOfCallableWrapper<C, R>(threadExecutor.submit(callable), callable);
	}

	@Override
	public abstract int getFreeThreadsCount();

	@Override
	public abstract void releaseNodeThreads(int nodeThreadsToRelease);

	/**
	 * Waits for all currently running graphs are already done
	 * and finishes graph executor life cycle.
	 * New graphs cannot be submitted after free invocation.
	 */
	@Override
	public void free() {
		watchdogExecutor.shutdown();
		nodeExecutor.shutdown();
	}

	/**
	 * Immediately finishes graph executor life cycle. All running
	 * graphs are aborted.
	 */
	@Override
	public void freeNow() {
		watchdogExecutor.shutdownNow();
		nodeExecutor.shutdownNow();
		threadExecutor.shutdownNow();
	}
	
	/**
     * Released thread is double checked whether no context in ContextProvider is managed.
     * @link {@link Executors#newCachedThreadPool(ThreadFactory))}
     */
    protected ExecutorService newCachedThreadPool(ThreadFactory threadFactory) {
        return new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                                      60L, TimeUnit.SECONDS,
                                      new SynchronousQueue<Runnable>(),
                                      threadFactory) {
        	@Override
        	protected void afterExecute(Runnable r, Throwable t) {
        		super.afterExecute(r, t);
        		
        		cleanThreadLocal();
        	}
        };
    }

    /**
     * Released thread is double checked whether no context in ContextProvider is managed.
     * @link {@link Executors#newCachedThreadPool()}
     */
    protected ExecutorService newCachedThreadPool() {
        return new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                                      60L, TimeUnit.SECONDS,
                                      new SynchronousQueue<Runnable>()) {
        	@Override
        	protected void afterExecute(Runnable r, Throwable t) {
        		super.afterExecute(r, t);
        		
        		cleanThreadLocal();
        	}
        };
    }
    
    protected abstract void cleanThreadLocal();
	
	/**
	 * Thread factory for watchdog thread pool.
	 */
	protected class WatchdogThreadFactory implements ThreadFactory {

		public WatchdogThreadFactory() {
		}

		@Override
		public Thread newThread(Runnable r) {
			return new Thread(r, WatchDog.WATCHDOG_THREAD_NAME_PREFIX);
		}
		
	}
	
	/**
	 * Thread factory for node thread pool.
	 */
	protected class NodeThreadFactory implements ThreadFactory {

		public NodeThreadFactory() {
		}

		@Override
		public Thread newThread(Runnable r) {
			Thread thread = new Thread(r, "Node");
			thread.setDaemon(true);
			thread.setPriority(1);
			return thread;
		}
		
	}
	
	private class FutureWrapper<R> implements Future<R> {
    	private Future<R> innerFuture;
    	
		public FutureWrapper(Future<R> innerFuture) {
			this.innerFuture = innerFuture;
		}
    	
		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return innerFuture.cancel(mayInterruptIfRunning);
		}

		@Override
		public boolean isCancelled() {
			return innerFuture.isCancelled();
		}

		@Override
		public boolean isDone() {
			return innerFuture.isDone();
		}

		@Override
		public R get() throws InterruptedException, ExecutionException {
			return innerFuture.get();
		}

		@Override
		public R get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			return innerFuture.get(timeout, unit);
		}
    }
	
	private class FutureOfRunnableWrapper<T extends Runnable> extends FutureWrapper<Object> implements FutureOfRunnable<T> {
    	private T runnable;
    	
		public FutureOfRunnableWrapper(Future<Object> innerFuture, T runnable) {
			super(innerFuture);
			
			this.runnable = runnable;
		}
		
		@Override
		public T getRunnable() {
			return runnable;
		}
    }
	
	protected class FutureOfCallableWrapper<C extends Callable<R>, R> extends FutureWrapper<R> implements FutureOfCallable<C, R> {
    	private C callable;
    	
		public FutureOfCallableWrapper(Future<R> innerFuture, C callable) {
			super(innerFuture);
			
			this.callable = callable;
		}
		
		@Override
		public C getCallable() {
			return callable;
		}
    }

}
