package org.jetel.graph.runtime;

import org.apache.log4j.MDC;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.Node;

/**
 * This implementation of Runnable interface should be use for each thread inside a
 * component.
 * 
 * @see ContextProvider
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 24.9.2009
 */
public abstract class CloverWorker implements Runnable {

	private Node node;
	
	private String name;
	
	private Thread thread;
	
	/**
	 * True if the worker is already running. This variable is guarded by 'this' monitor.
	 */
	private boolean isRunning;
	
	public CloverWorker(Node node, String name) {
		this.node = node;
		this.name = name;
		this.isRunning = false;
	}
	
	public void run() {
		ContextProvider.registerNode(node);
		MDC.put("runId", node.getGraph().getRuntimeContext().getRunId());
		Thread.currentThread().setName(node.getId() + ":" + name);
		synchronized (this) {
			isRunning = true;
			notifyAll();
		}
		try {
			work();
		} finally {
			ContextProvider.unregister();
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
	
	public abstract void work();
	
	public Thread startWorker() {
		thread = new Thread(this);
		thread.setDaemon(true);
		thread.start();
		node.registerChildThread(thread); //register worker as a child thread of this component
		return thread;
	}

	public Thread getThread() {
		return thread;
	}
	
}
