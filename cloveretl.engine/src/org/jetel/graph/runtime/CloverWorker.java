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
		thread.setPriority(Thread.MIN_PRIORITY);
		thread.setDaemon(false);
		thread.start();
		node.registerChildThread(thread); //register worker as a child thread of this component
		return thread;
	}

	public Thread getThread() {
		return thread;
	}
	
	public String getName() {
		return name;
	}
	
}
