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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
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

	private ExecutorService executor = Executors.newCachedThreadPool(new GraphThreadFactory());
	
	
	/**
	 * Waits for all currently running graphs are already done
	 * and finishes graph executor life cycle.
	 * New graphs cannot be submitted after free invocation.
	 */
	public void free() {
		executor.shutdown();
	}

	/**
	 * Immediately finishes graph executor life cycle. All runnig
	 * graphs are aborted.
	 */
	public void freeNow() {
		executor.shutdownNow();
	}

	/**
	 * Runs the given transformation graph in the given context.
	 * 
	 * @param graphURL URL, where should be XML graph specification stored
	 * @param context runtime context in which graph will executed
	 * @param password password for encrypting some hidden part of graphs, i.e. connections password can be encrypted
	 * @return future result
	 * @throws ComponentNotReadyException if occurs any initialization problem
     * @throws XMLConfigurationException deserialization from XML fails for any reason.
     * @throws GraphConfigurationException misconfigured graph
	 */
	public Future<Result> runGraph(URL graphURL, IGraphRuntimeContext context, String password) throws XMLConfigurationException, GraphConfigurationException, IOException, ComponentNotReadyException {
		context = new UnconfigurableGraphRuntimeContext(context);
		
		return runGraph(graphURL.openStream(), context, password);
	}

	/**
	 * Runs the given transformation graph in the given context.
	 * 
	 * @param graphStream graph in XML form
	 * @param context runtime context in which graph will executed
	 * @param password password for encrypting some hidden part of graphs, i.e. connections password can be encrypted
	 * @return future result
	 * @throws ComponentNotReadyException if occurs any initialization problem
     * @throws XMLConfigurationException deserialization from XML fails for any reason.
     * @throws GraphConfigurationException misconfigured graph
	 */
	public Future<Result> runGraph(InputStream graphStream, IGraphRuntimeContext context, String password) 
	throws XMLConfigurationException, GraphConfigurationException, ComponentNotReadyException {
		context = new UnconfigurableGraphRuntimeContext(context);

		TransformationGraph graph = loadGraph(graphStream, context.getAdditionalProperties());
		
        graph.setPassword(password);

        return runGraph(graph, context);
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

        // check graph elements configuration
		
        logger.info("Checking graph configuration...");
        ConfigurationStatus status = graph.checkConfig(null);
        status.log();

        if(context.isCheckConfig()) {
        	return null;
        }
        
        graph.init();

        if (context.isVerboseMode()) {
            // this can be called only after graph.init()
            graph.dumpGraphConfiguration();
        }

        if (context.isVerboseMode()) {
            // this can be called only after graph.init()
            graph.dumpGraphConfiguration();
        }

//        long timestamp = System.currentTimeMillis();
        WatchDog watchDog = new WatchDog(graph, context);

        return executor.submit(watchDog);
        
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

	private static class GraphThreadFactory implements ThreadFactory {

		public Thread newThread(Runnable r) {
			return new Thread(r, "WatchDog");
		}
		
	}
	
}
