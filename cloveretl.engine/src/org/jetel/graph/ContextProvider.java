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

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.log4j.Logger;
import org.jetel.graph.runtime.CloverWorker;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.IAuthorityProxy;

/**
 * <p>
 * This class should be able to provide org.jetel.graph.Node or org.jetel.graph.TransformationGraph
 * corresponding to the current thread. Both are provided via static methods - {@link #getNode()}
 * and {@link #getGraph()}.
 * <p>
 * Complete stack of contexts is managed for each thread by this {@link ContextProvider}.
 * So registering new context ({@link #registerGraph(TransformationGraph)}
 * or {@link #registerNode(Node)}) is just adding new context to the thread corresponding stack.
 * The last registered context is actual and provided be {@link #getGraph()} and {@link #getNode()} methods.
 * Calling method {@link #unregister(Context)} just remove the top context from the stack and the former context
 * is taken into account.
 * <p>
 * Example of usage:
 * <pre>
 * Node component = ...
 * Context c = ContextProvider.registerNode(component);
 * try {
 *   doSomeWork(component);
 * } finally {
 *   ContextProvider.unregister(c);
 * }
 * </pre>
 * <p>
 * Both registering methods returns {@link Context} for further unregistering purpose. This Context is
 * important for validation of usage correctness and for possible fail-over. To avoid inconsistency in context
 * stack, the presented template above should be used.
 * <p>
 * The {@link ContextProvider} can work only when all threads working with graph elements are registered.
 * <p>
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

	private final static Logger logger = Logger.getLogger(ContextProvider.class);

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

	/**
	 * Returns componentId from thread context. If it's missing, returns null.
	 */
	public static String getComponentId() {
		Node component = ContextProvider.getNode();
		return component==null ? null : component.getId();
	}
	
	/**
	 * @return contextURL from {@link GraphRuntimeContext} associated with current graph or 
	 * <code>null</code> if no graph is on thread context
	 */
	public static URL getContextURL() {
		TransformationGraph graph = getGraph();
		return graph != null ? graph.getRuntimeContext().getContextURL() : null;
	}
	
	/**
	 * @return {@link IAuthorityProxy} implementation associated with current graph or default authority proxy
	 * if no graph is registered with this thread
	 */
	public static IAuthorityProxy getAuthorityProxy() {
		return IAuthorityProxy.getAuthorityProxy(getGraph());
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
	 * Add new component based context to thread corresponding stack of contexts.
	 * @return Context instance, which should be passed for de-registration in {@link #unregister(Context)} method
	 */
	public static synchronized Context registerNode(Node node) {
		if (node == null || node.getGraph() == null) {
			return null;
		}
		Context newContext = new Context(node, node.getGraph());
		registerContext(newContext);
		return newContext;
	}

	/**
	 * Add new graph based context to thread corresponding stack of contexts.
	 * @return Context instance, which should be passed for de-registration in {@link #unregister(Context)} method
	 */
	public static synchronized Context registerGraph(TransformationGraph graph) {
		if (graph == null) {
			return null;
		}
		Context newContext = new Context(null, graph);
		registerContext(newContext);
		return newContext;
	}

	private static void registerContext(Context context) {
		assert (context != null);
		Stack<Context> threadCache = contextCache.get(Thread.currentThread());
		if (threadCache == null) {
			threadCache = new Stack<Context>();
			contextCache.put(Thread.currentThread(), threadCache);
		}
		threadCache.push(context);
	}
	
	/**
	 * Unregister last registered context associated with current thread - the last context
	 * has to be passed to validate right usage of ContextProvider.
	 */
	public static synchronized void unregister(Context requestedContext) {
		if (requestedContext == null) {
			//DO NOTHING
			return;
		}
		Stack<Context> threadCache = contextCache.get(Thread.currentThread());
		if (threadCache == null) {
			logger.error("Illegal state in ContextProvider. No cached contexts for current thread " + Thread.currentThread().getName() + ". Requested context: " + requestedContext);
			return;
		}
		if (!threadCache.isEmpty()) {
			Context deregisteredContext = threadCache.pop();
			if (deregisteredContext != requestedContext) {
				if (threadCache.contains(requestedContext)) {
					logger.error("Illegal state in ContextProvider. Context, which should be unregistered, is not on top of stack. Recovery is perfomed, context " + deregisteredContext + " is removed from stack.");
					while (deregisteredContext != requestedContext) {
						deregisteredContext = threadCache.pop();
						logger.error("Illegal state in ContextProvider. Context, which should be unregistered, is not on top of stack. Recovery is perfomed, context " + deregisteredContext + " is removed from stack.");
					}
					logger.error("Illegal state in ContextProvider. Context, which should be unregistered, is not on top of stack. Recovery finished. Requested context is " + requestedContext);
				} else {
					threadCache.push(deregisteredContext);
					logger.error("Illegal state in ContextProvider. Last context in stack was " + deregisteredContext + " but requested context is " + requestedContext);
					return;
				}
			}
			if (threadCache.isEmpty()) {
				contextCache.remove(Thread.currentThread());
			}
		} else {
			contextCache.remove(Thread.currentThread());
			logger.error("Illegal state in ContextProvider. Empty context cache.");
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
	 */
	public static class Context {
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
		
		@Override
		public String toString() {
			if (node != null) {
				return "context(component:" + node.toString() + ", graph:" + graph.toString() + ")";
			} else {
				return "context(graph:" + graph.toString() + ")";
			}
		}
	}
	
}
