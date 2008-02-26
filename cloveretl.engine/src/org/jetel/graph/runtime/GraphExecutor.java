/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.graph.runtime;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;

/**
 * Graph executor is serving to process transformation graphs.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 27.11.2007
 */
public class GraphExecutor {

	private static Log logger = LogFactory.getLog(GraphExecutor.class);

	private static int DEFAULT_MAX_GRAPHS_QUEUE_SIZE = 10;
	
	/**
	 * Maximum size of queue for scheduled graphs.
	 */
	private int maxGraphsQueueSize = DEFAULT_MAX_GRAPHS_QUEUE_SIZE;
	
	private ThreadPoolExecutor watchdogExecutor; 
	
	private ThreadPoolExecutor nodeExecutor;
	
	private int maxNodes = 0;
	
	private int maxGraphs = 0;
	
	private int runningNodes = 0;
	
	/**
	 * Constructor for default graph executor without thread limits.
	 */
	public GraphExecutor() {
		this(0,0); //graph executor without any limits
	}

	/**
	 * Graph executor's constructor.
	 * @param maxGraphs maximum number of simultaneously running graphs; zero means without limit
	 * @param maxNodes maximum number of simultaneously running nodes; zero means without limit
	 */
	public GraphExecutor(int maxGraphs, int maxNodes) {
		this.maxGraphs = maxGraphs;
		this.maxNodes = maxNodes;
		
		if(maxGraphs <= 0) {
			watchdogExecutor = (ThreadPoolExecutor) Executors.newCachedThreadPool(new WatchdogThreadFactory());
		} else {
			watchdogExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxGraphs, new WatchdogThreadFactory());
		}
		if(maxNodes <= 0) {
			nodeExecutor = (ThreadPoolExecutor) Executors.newCachedThreadPool(new NodeThreadFactory());
		} else {
			nodeExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxNodes, new NodeThreadFactory());
		}
	}
	
	/**
	 * Waits for all currently running graphs are already done
	 * and finishes graph executor life cycle.
	 * New graphs cannot be submitted after free invocation.
	 */
	public void free() {
		watchdogExecutor.shutdown();
		nodeExecutor.shutdown();
	}

	/**
	 * Immediately finishes graph executor life cycle. All running
	 * graphs are aborted.
	 */
	public void freeNow() {
		watchdogExecutor.shutdownNow();
		nodeExecutor.shutdownNow();
	}

	/**
	 * Prepares graph for first run. Checks configuration and initializes.
	 * @param graph
	 * @throws ComponentNotReadyException
	 */
	public static void initGraph(TransformationGraph graph) throws ComponentNotReadyException {
		logger.info("Checking graph configuration...");
		ConfigurationStatus status = graph.checkConfig(null);
		status.log();
        graph.init();
	}
	
	/**
	 * Runs the given transformation graph in the given context.
	 * 
	 * @param graph graph to execute
	 * @param context runtime context in which graph will executed
	 * @return future result or null if graph was not be processed
	 * @throws ComponentNotReadyException if occurs any initialization problem
	 */
	public Future<Result> runGraph(TransformationGraph graph, IGraphRuntimeContext context) 
	throws ComponentNotReadyException {

		//runtime exception is thrown in case too many graphs scheduled 
		if(watchdogExecutor.getQueue().size() > maxGraphsQueueSize) {
			throw new RuntimeException("Out of resources to run a graph.");
		}

		context = new UnconfigurableGraphRuntimeContext(context);

		if (!graph.isInitialized()) {
			initGraph(graph);
		}

//        // check graph elements configuration
//        if(context.isCheckConfig()) {
//            logger.info("Checking graph configuration...");
//            ConfigurationStatus status = graph.checkConfig(null);
//            status.log();
//        }
//        
//        graph.init();

        if (context.isVerboseMode()) {
            // this can be called only after graph.init()
            graph.dumpGraphConfiguration();
        }

        if (context.isVerboseMode()) {
            // this can be called only after graph.init()
            graph.dumpGraphConfiguration();
        }

        WatchDog watchDog = new WatchDog(this, graph, context);
        return watchdogExecutor.submit(watchDog);
	}

	/**
	 * Runs given runnable class via inner instance of executor service.
	 * It suspects that the given runnable instance is a node representation.
	 * @param runnable
	 */
	synchronized public void executeNode(Runnable node) {
		nodeExecutor.execute(node);
		runningNodes++;
	}
	
	/**
	 * Returns the approximate number of available free threads.
	 * @return number of threads
	 */
	synchronized public int getFreeThreadsCount() {
		if(maxNodes > 0) {
			return maxNodes - runningNodes;
		} else {
			return Integer.MAX_VALUE;
		}
	}
	
	/**
	 * Decreases number of used threads.
	 * @param nodeThreadsToRelease
	 */
	synchronized public void releaseNodeThreads(int nodeThreadsToRelease) {
		runningNodes -= nodeThreadsToRelease;
	}
	
    /**
     * Instantiates transformation graph from a given input stream and presets a given properties.
     * @param graphStream graph in XML form stored in character stream
     * @param properties additional properties
     * @return transformation graph
     * @throws XMLConfigurationException deserialization from XML fails for any reason.
     * @throws GraphConfigurationException misconfigured graph
     */
	public static TransformationGraph loadGraph(InputStream graphStream, Properties properties)
	throws XMLConfigurationException, GraphConfigurationException {
        TransformationGraphXMLReaderWriter graphReader = new TransformationGraphXMLReaderWriter(properties);
        return graphReader.read(graphStream);
    }

	/**
	 * 
	 * @param graphStream
	 * @param properties
	 * @param password
	 * @return
	 * @throws XMLConfigurationException
	 * @throws GraphConfigurationException
	 */
	public static TransformationGraph loadGraph(InputStream graphStream, Properties properties, String password)
	throws XMLConfigurationException, GraphConfigurationException {
		TransformationGraph graph = GraphExecutor.loadGraph(graphStream, properties);
        graph.setPassword(password);
        return graph; 
    }
	
	private static class WatchdogThreadFactory implements ThreadFactory {

		public Thread newThread(Runnable r) {
			return new Thread(r, "WatchDog");
		}
		
	}

	private static class NodeThreadFactory implements ThreadFactory {

		public Thread newThread(Runnable r) {
			Thread thread = new Thread(r, "Node");
			thread.setDaemon(true);
			thread.setPriority(1);
			return thread;
		}
		
	}

	/**
	 * Sets maximum size of queued graphs.
	 * @param maxGraphsQueueSize
	 */
	public void setMaxGraphsQueueSize(int maxGraphsQueueSize) {
		this.maxGraphsQueueSize = maxGraphsQueueSize;
	}

}
