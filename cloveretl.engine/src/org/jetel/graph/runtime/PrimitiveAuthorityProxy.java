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
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.main.runGraph;
import org.jetel.util.FileConstrains;
import org.jetel.util.file.FileUtils;

/**
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jul 11, 2008
 */
public class PrimitiveAuthorityProxy implements IAuthorityProxy {

	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#getSharedSequence(org.jetel.data.sequence.Sequence)
	 */
	public Sequence getSharedSequence(Sequence sequence) {
		return sequence;
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#freeSharedSequence(org.jetel.data.sequence.Sequence)
	 */
	public void freeSharedSequence(Sequence sequence) {
		sequence.free();
	}

	/**
	 * Implementation taken from original RunGraph component created by Juraj Vicenik.
	 * 
	 * @see org.jetel.graph.runtime.IAuthorityProxy#executeGraph(long, java.lang.String)
	 */
	public RunResult executeGraph(long runId, String graphFileName, Properties props, String logFile) {
        RunResult rr = new RunResult();

		InputStream in = null;		

		try {
            in = Channels.newInputStream(FileUtils.getReadableChannel(null, graphFileName));
        } catch (IOException e) {
        	rr.description = "Error - graph definition file can't be read: " + e.getMessage();
        	rr.result = Result.ERROR;
        	return rr;
        }
        
        GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
        runtimeContext.setRunId(getUniqueRunId(runId));
        runtimeContext.setLogLevel(Level.ALL);
        runtimeContext.setLogLocation(logFile);
        if (logFile != null) {
        	prepareLogger(runtimeContext);
        }
        if (props != null)
        	runtimeContext.addAdditionalProperties(props);
        Future<Result> futureResult = null;                

        // TODO - hotfix - clover can't run two graphs simultaneously with enable edge debugging
		// after resolve issue 1748 (http://home.javlinconsulting.cz/view.php?id=1748) next line should be removed
        runtimeContext.setDebugMode(false);
        
        TransformationGraph graph = null;
		try {
			graph = TransformationGraphXMLReaderWriter.loadGraph(in, runtimeContext.getAdditionalProperties());
        } catch (XMLConfigurationException e) {
        	rr.description = "Error in reading graph from XML !" + e.getMessage();
        	rr.result = Result.ERROR;
        	return rr;
        } catch (GraphConfigurationException e) {
        	rr.description = "Error - graph's configuration invalid !" + e.getMessage();
        	rr.result = Result.ERROR;
        	return rr;
		} 

        long startTime = System.currentTimeMillis();
        Result result = Result.N_A;
        try {
    		try {
    			EngineInitializer.initGraph(graph, runtimeContext);
    			futureResult = runGraph.executeGraph(graph, runtimeContext);

    		} catch (ComponentNotReadyException e) {
    			rr.description = "Error during graph initialization: " + e.getMessage();           
            	rr.result = Result.ERROR;
            	return rr;
            } catch (RuntimeException e) {
            	rr.description = "Error during graph initialization: " +  e.getMessage();           
            	rr.result = Result.ERROR;
            	return rr;
            }
            
    		try {
    			result = futureResult.get();
    		} catch (InterruptedException e) {
    			rr.description = "Graph was unexpectedly interrupted !" + e.getMessage();            
            	rr.result = Result.ERROR;
            	return rr;
    		} catch (ExecutionException e) {
    			rr.description = "Error during graph processing !" + e.getMessage();            
            	rr.result = Result.ERROR;
            	return rr;
    		}
        } finally {
    		if (graph != null)
    			graph.free();
        }
		
        long totalTime = System.currentTimeMillis() - startTime;
        rr.result = result;
        rr.duration = totalTime;
        
		return rr;
	}
	
	private static long getUniqueRunId(long parentRunId) {
		Random random = new Random();
		long runId = Math.abs((random.nextLong() % 999));
		return (runId != parentRunId) ? runId : runId + 1;
		// TODO returned runId mustn't be unique
	}
	
	/**
	 * Set file that will be used for logging graph.
	 * Information about graph, log level and log location are in graph runtime context
	 * 
	 * @param runtimeContext
	 */
	private static void prepareLogger(GraphRuntimeContext runtimeContext) {
		FileAppender logAppender = null;
		try {
			logAppender = new FileAppender(new PatternLayout("%d %-5p %-3X{runId} [%t] %m%n"), runtimeContext.getLogLocation());
			Filter f = new GraphLogFilter(runtimeContext.getRunId(), runtimeContext.getLogLevel());
			logAppender.addFilter(f);
			Logger.getRootLogger().addAppender(logAppender);
		} catch (Exception e) {
			Logger.getRootLogger().error("logger initialization", e);
		}
	}
	
	/**
	 * Note: This class is copied from Clover server.
	 * 
	 * Use instance of this filter for log appender which should collect messages only from specified graph execution.
	 * Execution is specified by runId.
	 * We expect, that this value is preset in MDC for each related thread.
	 * 
	 * @author Martin Varecha <martin.varecha@javlinconsulting.cz>
	 * (c) JavlinConsulting s.r.o.
	 * www.javlinconsulting.cz
	 * @created Mar 5, 2008
	 */
	private static class GraphLogFilter extends Filter{
		private long runId;
		private Level logLevel;
	 
		public GraphLogFilter(long runId, Level logLevel) {
			super();
			this.runId = runId;
			this.logLevel = logLevel;
		}

		@Override
		public int decide(LoggingEvent event) {
			Object oRunId = event.getMDC("runId");
			if (!(oRunId instanceof Long))
				return Filter.DENY;
			Long rId = (Long)oRunId;
			if (this.runId != rId.longValue())
				return Filter.DENY;
			else {
				if (event.getLevel().isGreaterOrEqual(logLevel))
					return Filter.ACCEPT;
				else
					return Filter.DENY;
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#getSandboxResourceInput(long, java.lang.String, java.lang.String)
	 */
	public InputStream getSandboxResourceInput(long runId, String storageCode, String path) {
		throw new UnsupportedOperationException("Sandbox resources are accessible only in CloverETL Server environment!");
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#getSandboxResourceOutput(long, java.lang.String, java.lang.String)
	 */
	public OutputStream getSandboxResourceOutput(long runId, String storageCode, String path) {
		throw new UnsupportedOperationException("Sandbox resources are accessible only in CloverETL Server environment!");
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#getPartitionedSandboxResourceInput(long, java.lang.String, java.lang.String)
	 */
	public InputStream[] getPartitionedSandboxResourceInput(long runId, String storageCode, String path) {
		throw new UnsupportedOperationException("Sandbox resources are accessible only in CloverETL Server environment!");
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#getPartitionedSandboxResourceOutput(long, java.lang.String, java.lang.String)
	 */
	public OutputStream[] getPartitionedSandboxResourceOutput(long runId, String storageCode, String path) {
		throw new UnsupportedOperationException("Sandbox resources are accessible only in CloverETL Server environment!");
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#getSlaveOutputStreams(long)
	 */
	public OutputStream[] getClusterPartitionerOutputStreams(long runId, String componentId) throws IOException {
		throw new UnsupportedOperationException("ClusterPartitioner output streams are available only in CloverETL Server environment!");
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#getSlaveInputStream(long)
	 */
	public InputStream getClusterPartitionerInputStream(long runId, String componentId) throws IOException {
		throw new UnsupportedOperationException("ClusterPartitioner input stream is available only in CloverETL Server environment!");
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#getClusterGatherInputStreams(long, java.lang.String)
	 */
	public InputStream[] getClusterGatherInputStreams(long runId, String componentId) throws IOException {
		throw new UnsupportedOperationException("ClusterGather input streams are available only in CloverETL Server environment!");
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#getClusterGatherOutputStream(long, java.lang.String)
	 */
	public OutputStream getClusterGatherOutputStream(long runId, String componentId) throws IOException {
		throw new UnsupportedOperationException("ClusterGather output stream is available only in CloverETL Server environment!");
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#isMaster(long)
	 */
	public boolean isPrimaryWorker(long runId) {
		throw new UnsupportedOperationException("Primary worker does has sense only in CloverETL Server environment!");
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#assignFilePortion(long, java.lang.String, java.lang.String)
	 */
	public FileConstrains assignFilePortion(long runId, String componentId, String fileURL) throws IOException {
		return null;
	}
	
}
