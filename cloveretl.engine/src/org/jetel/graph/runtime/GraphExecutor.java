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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

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

	private ExecutorService watchdogExecutor = Executors.newCachedThreadPool(new WatchdogThreadFactory());
	
	private ExecutorService nodeExecutor = Executors.newCachedThreadPool(new NodeThreadFactory());
	
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
	public void initGraph(TransformationGraph graph) throws ComponentNotReadyException {
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

//        long timestamp = System.currentTimeMillis();
        WatchDog watchDog = new WatchDog(this, graph, context);

        return watchdogExecutor.submit(watchDog);
        
//        logger.info("Starting WatchDog thread ...");
//        watchDog.start();
//        try {
//            watchDog.join();
//        } catch (InterruptedException ex) {
//            logger.error(ex);
//            return Result.ABORTED;
//        }
//        logger.info("WatchDog thread finished - total execution time: "
//                + (System.currentTimeMillis() - timestamp) / 1000 + " (sec)");
//
//        freeResources();
//
//        switch (watchDog.getStatus()) {
//        case FINISHED_OK:
//            logger.info("Graph execution finished successfully");
//            break;
//        case ABORTED:
//            logger.error("!!! Graph execution aborted !!!");
//            break;
//        case ERROR:
//            logger.error("!!! Graph execution finished with errors !!!");
//            break;
//        default:
//            logger.fatal("Unexpected result when executing graph !");
//        }
//        
//        return watchDog.getStatus();
	}

	/**
	 * Runs given runnable class via inner instance of executor service.
	 * It suspects that the given runnable instance is a node representation.
	 * @param runnable
	 */
	public void executeNode(Runnable node) {
		nodeExecutor.execute(node);
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
			return thread;
		}
		
	}

}
