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
package org.jetel.graph;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.jetel.graph.runtime.CloverWorker;

/**
 * This class should be able to provide org.jetel.graph.Node or org.jetel.graph.TransformationGraph
 * corresponding to the current thread. Both are provided via static methods - {@link #getNode()}
 * and {@link #getGraph()}.
 *
 * This functionality can work only when all threads working with graph elements are registered 
 * in this class.
 *
 * For this purpose it is recommended to use CloverWorker class instead Runnable every time 
 * you want to create separate thread inside a component.
 *
 * @see CloverWorker
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 24.9.2009
 */
public class ContextProvider {

	private static final Map<Thread, Stack<Context>> contextCache = new HashMap<Thread, Stack<Context>>(); 
	
	/**
	 * @return transformation graph associated with current thread or <code>null</code>
	 */
	public static synchronized TransformationGraph getGraph() {
		Context context = getContext();
		return (context != null) ? context.getGraph() : null;
    }

	/**
	 * @return component associated with current thread or <code>null</code>
	 */
	public static synchronized Node getNode() {
		Context context = getContext();
		return (context != null) ? context.getNode() : null;
		
//    	return nodesCache.get(Thread.currentThread());
    }

	private static Context getContext() {
		Stack<Context> threadCache = contextCache.get(Thread.currentThread());
		if (threadCache != null) {
			assert(!threadCache.isEmpty()); //threadCache cannot be empty - it is class ContextProvider invariant
			return threadCache.peek();
		} else {
			return null;
		}
	}
	
	/**
	 * @return job type of current graph or {@link JobType#DEFAULT} if current graph cannot be specified
	 */
	public static synchronized JobType getJobType() {
    	TransformationGraph currentGraph = ContextProvider.getGraph();
    	
    	if (currentGraph != null) {
    		return currentGraph.getJobType();
    	} else {
    		return JobType.DEFAULT;
    	}
	}
	
	/**
	 * Associates the given component with current thread.
	 */
	public static synchronized void registerNode(Node node) {
		registerContext(new Context(node, node.getGraph()));
	}

	/**
	 * Associates the given graph with current thread.
	 */
	public static synchronized void registerGraph(TransformationGraph graph) {
		registerContext(new Context(null, graph));
	}

	private static void registerContext(Context context) {
		Stack<Context> threadCache = contextCache.get(Thread.currentThread());
		if (threadCache == null) {
			threadCache = new Stack<Context>();
			contextCache.put(Thread.currentThread(), threadCache);
		}
		threadCache.push(context);
	}
	
	/**
	 * Unregister last registered context associated with current thread.
	 */
	public static synchronized void unregister() {
		Stack<Context> threadCache = contextCache.get(Thread.currentThread());
		if (!threadCache.isEmpty()) {
			threadCache.pop();
		}
		if (threadCache.isEmpty()) {
			contextCache.remove(Thread.currentThread());
		}
	}

	/**
	 * Full context stack for current thread is released.
	 */
	public static synchronized void unregisterAll() {
		contextCache.remove(Thread.currentThread());
	}
	
	/**
	 * This class represents all context information managed by {@link ContextProvider}.
	 * Can be used in following pattern:<br>
	 * <pre>
	 * ContextProvider.Context formerContext = ContextProvider.getContext();
	 * try {
	 *   ContextProvider.regitesterNode(node);
	 *   ...
	 * } finally {
	 *   ContextProvider.setContext(formerContext);
	 * }
	 * </pre>
	 */
	private static class Context {
		private Node node;
		private TransformationGraph graph;
		Context(Node node, TransformationGraph graph) {
			this.node = node;
			this.graph = graph;
		}
		public Node getNode() {
			return node;
		}
		public TransformationGraph getGraph() {
			return graph;
		}
	}
	
}
