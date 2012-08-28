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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.TempFileCreationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.IGraphElement;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.graph.dictionary.DictionaryValuesContainer;
import org.jetel.graph.runtime.jmx.TrackingEvent;
import org.jetel.main.runGraph;
import org.jetel.util.FileConstrains;
import org.jetel.util.MiscUtils;
import org.jetel.util.bytes.SeekableByteChannel;
import org.jetel.util.file.FileUtils;

/**
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jul 11, 2008
 */
public class PrimitiveAuthorityProxy extends IAuthorityProxy {

	/**
	 * Suffix of temp files created by standalone engine
	 */
	private static final String CLOVER_TMP_FILE_SUFFIX = ".clover.tmp";

	public PrimitiveAuthorityProxy(){
		super();
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#getSharedSequence(org.jetel.data.sequence.Sequence)
	 */
	@Override
	public Sequence getSharedSequence(Sequence sequence) {
		return sequence;
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#freeSharedSequence(org.jetel.data.sequence.Sequence)
	 */
	@Override
	public void freeSharedSequence(Sequence sequence) {
		sequence.free();
	}
	
	private GraphRuntimeContext prepareRuntimeContext(GraphRuntimeContext givenRuntimeContext, long runId) {
        GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
        runtimeContext.setRunId(runId);
        runtimeContext.setLogLevel(Level.ALL);
        if (runtimeContext.getLogLocation() != null) {
        	prepareLogger(runtimeContext);
        }
        runtimeContext.setAdditionalProperties(givenRuntimeContext.getAdditionalProperties());
        runtimeContext.setContextURL(givenRuntimeContext.getContextURL());
        runtimeContext.setDictionaryContent(givenRuntimeContext.getDictionaryContent());
        
        // TODO - hotfix - clover can't run two graphs simultaneously with enable edge debugging
		// after resolve issue CL-416 (https://bug.javlin.eu/browse/CL-416) next line should be removed
        runtimeContext.setDebugMode(false);
        
        return runtimeContext;
	}

	private RunStatus executeGraphSync(RunStatus rr, TransformationGraph graph, GraphRuntimeContext runtimeContext, Long timeout) {
		Future<Result> futureResult = null;                
        Result result = Result.N_A;
        try {
    		try {
    			EngineInitializer.initGraph(graph);
    			futureResult = runGraph.executeGraph(graph, runtimeContext);

    		} catch (ComponentNotReadyException e) {
            	rr.endTime = new Date(System.currentTimeMillis());
            	rr.duration = rr.endTime.getTime() - rr.startTime.getTime(); 
    			rr.errMessage = MiscUtils.exceptionChainToMessage("Error during graph initialization.", e);           
            	rr.errException = MiscUtils.stackTraceToString(e);
            	rr.status = Result.ERROR;
            	return rr;
            } catch (RuntimeException e) {
            	rr.endTime = new Date(System.currentTimeMillis());
            	rr.duration = rr.endTime.getTime() - rr.startTime.getTime(); 
            	rr.errMessage = MiscUtils.exceptionChainToMessage("Error during graph initialization.", e);           
            	rr.errException = MiscUtils.stackTraceToString(e);
            	rr.status = Result.ERROR;
            	return rr;
            }
            
    		try {
    			if (timeout == null) {
    				result = futureResult.get();
    			} else {
    				result = futureResult.get(timeout, TimeUnit.MILLISECONDS);
    			}
    		} catch (TimeoutException e) {
    			graph.getWatchDog().abort();
    			
            	rr.endTime = new Date(System.currentTimeMillis());
            	rr.duration = rr.endTime.getTime() - rr.startTime.getTime(); 
    			rr.errMessage = "Timeout expired ! (" + timeout + " ms)";            
            	rr.status = Result.ABORTED;
            	return rr;
    		} catch (InterruptedException e) {
            	rr.endTime = new Date(System.currentTimeMillis());
            	rr.duration = rr.endTime.getTime() - rr.startTime.getTime(); 
    			rr.errMessage = MiscUtils.exceptionChainToMessage("Graph was unexpectedly interrupted !", e);            
            	rr.errException = MiscUtils.stackTraceToString(e);
            	rr.status = Result.ERROR;
            	return rr;
    		} catch (ExecutionException e) {
            	rr.endTime = new Date(System.currentTimeMillis());
            	rr.duration = rr.endTime.getTime() - rr.startTime.getTime(); 
    			rr.errMessage = MiscUtils.exceptionChainToMessage("Error during graph processing !", e);            
            	rr.errException = MiscUtils.stackTraceToString(e);
            	rr.status = Result.ERROR;
            	return rr;
    		}
    		IGraphElement causeGraphElement = graph.getWatchDog().getCauseGraphElement();
    		rr.endTime = new Date(System.currentTimeMillis());
        	rr.duration = rr.endTime.getTime() - rr.startTime.getTime(); 
            rr.dictionaryOut = DictionaryValuesContainer.getInstance(graph.getDictionary());
            rr.tracking = graph.getWatchDog().getCloverJmx().getGraphTracking();
            rr.status = result;
    		rr.errMessage = graph.getWatchDog().getErrorMessage();            
        	rr.errException = MiscUtils.stackTraceToString(graph.getWatchDog().getCauseException());
        	rr.errComponent = causeGraphElement != null ? causeGraphElement.getId() : null;
        	rr.errComponentType = (causeGraphElement instanceof Node) ? ((Node) causeGraphElement).getType() : null;
        } finally {
    		if (graph != null)
    			graph.free();
        }
		
		return rr;
	}

	/**
	 * Implementation taken from original RunGraph component created by Juraj Vicenik.
	 * 
	 * @see org.jetel.graph.runtime.IAuthorityProxy#executeGraph(long, java.lang.String)
	 */
	@Override
	public RunStatus executeGraphSync(String graphFileName, GraphRuntimeContext givenRuntimeContext, Long timeout) {
		RunStatus rr = new RunStatus();
        long runId = (this.runtimeContext == null) ? 0:this.runtimeContext.getRunId();
        
		InputStream in = null;		

		long startTime = System.currentTimeMillis();
		rr.startTime = new Date(startTime);
		
		try {
            in = Channels.newInputStream(FileUtils.getReadableChannel(givenRuntimeContext.getContextURL(), graphFileName));
        } catch (IOException e) {
        	rr.endTime = new Date(System.currentTimeMillis());
        	rr.duration = rr.endTime.getTime() - rr.startTime.getTime(); 
        	rr.errMessage = MiscUtils.exceptionChainToMessage("Error - graph definition file can't be read!", e);
        	rr.errException = MiscUtils.stackTraceToString(e);
        	rr.status = Result.ERROR;
        	return rr;
        }
		
		GraphRuntimeContext runtimeContext = prepareRuntimeContext(givenRuntimeContext, rr.runId = getUniqueRunId(runId));
        runtimeContext.setUseJMX(givenRuntimeContext.useJMX());
        
        TransformationGraph graph = null;
		try {
			graph = TransformationGraphXMLReaderWriter.loadGraph(in, runtimeContext);
			rr.jobUrl = graphFileName;
        } catch (XMLConfigurationException e) {
        	rr.endTime = new Date(System.currentTimeMillis());
        	rr.duration = rr.endTime.getTime() - rr.startTime.getTime(); 
        	rr.errMessage = MiscUtils.exceptionChainToMessage("Error in reading graph from XML!", e);
        	rr.errException = MiscUtils.stackTraceToString(e);
        	rr.status = Result.ERROR;
        	return rr;
        } catch (GraphConfigurationException e) {
        	rr.endTime = new Date(System.currentTimeMillis());
        	rr.duration = rr.endTime.getTime() - rr.startTime.getTime(); 
        	rr.errMessage = MiscUtils.exceptionChainToMessage("Error - graph's configuration invalid!", e);
        	rr.errException = MiscUtils.stackTraceToString(e);
        	rr.status = Result.ERROR;
        	return rr;
		} 

		return executeGraphSync(rr, graph, runtimeContext, timeout);
	}
	
	@Override
	public RunStatus executeGraphSync(TransformationGraph graph, GraphRuntimeContext givenRuntimeContext, Long timeout)
			throws InterruptedException {
		RunStatus rr = new RunStatus();
        long runId = (this.runtimeContext == null) ? 0:this.runtimeContext.getRunId();
        
		long startTime = System.currentTimeMillis();
		rr.startTime = new Date(startTime);

		GraphRuntimeContext runtimeContext = prepareRuntimeContext(givenRuntimeContext, rr.runId = getUniqueRunId(runId));

		return executeGraphSync(rr, graph, runtimeContext, timeout);
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

	@Override
	public boolean makeDirectories(String storageCode, String path, boolean makeParents) {
		throw new UnsupportedOperationException("Sandbox directory may be created only in CloverETL Server environment!");
	}

	@Override
	public Collection<String> resolveAllFiles(String sandboxCode, String wildcardedPath) {
		throw new UnsupportedOperationException("Sandbox resources are accessible only in CloverETL Server environment!");
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#getSandboxResourceInput(java.lang.String, java.lang.String)
	 */
	@Override
	public InputStream getSandboxResourceInput(String storageCode, String path) {
		throw new UnsupportedOperationException("Sandbox resources are accessible only in CloverETL Server environment!");
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#getSandboxResourceOutput(java.lang.String, java.lang.String)
	 */
	@Override
	public OutputStream getSandboxResourceOutput(String storageCode, String path, boolean append) {
		throw new UnsupportedOperationException("Sandbox resources are accessible only in CloverETL Server environment!");
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#getPartitionedSandboxResourceInput(java.lang.String, java.lang.String)
	 */
	public InputStream[] getPartitionedSandboxResourceInput(String storageCode, String path) {
		throw new UnsupportedOperationException("Sandbox resources are accessible only in CloverETL Server environment!");
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#getPartitionedSandboxResourceOutput(java.lang.String, java.lang.String)
	 */
	public OutputStream[] getPartitionedSandboxResourceOutput(String storageCode, String path) {
		throw new UnsupportedOperationException("Sandbox resources are accessible only in CloverETL Server environment!");
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#getClusterPartitionerOutputStreams(java.lang.String)
	 */
	@Override
	public OutputStream[] getClusterPartitionerOutputStreams(String componentId) throws IOException {
		throw new UnsupportedOperationException("ClusterPartitioner output streams are available only in CloverETL Server environment!");
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#getClusterPartitionerInputStream(java.lang.String)
	 */
	@Override
	public InputStream getClusterPartitionerInputStream(String componentId) throws IOException {
		throw new UnsupportedOperationException("ClusterPartitioner input stream is available only in CloverETL Server environment!");
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#getClusterGatherInputStreams(java.lang.String)
	 */
	@Override
	public InputStream[] getClusterGatherInputStreams(String componentId) throws IOException {
		throw new UnsupportedOperationException("ClusterGather input streams are available only in CloverETL Server environment!");
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#getClusterGatherOutputStream(java.lang.String)
	 */
	@Override
	public OutputStream getClusterGatherOutputStream(String componentId) throws IOException {
		throw new UnsupportedOperationException("ClusterGather output stream is available only in CloverETL Server environment!");
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#isPrimaryWorker()
	 */
	@Override
	public boolean isPrimaryWorker() {
		throw new UnsupportedOperationException("Primary worker does has sense only in CloverETL Server environment!");
	}

	@Override
	public FileConstrains assignFilePortion(String componentId, String fileURL,
			SeekableByteChannel channel, Charset charset, String[] recordDelimiters) throws IOException {
		return null;
	}

	@Override
	public FileConstrains assignFilePortion(String componentId, String fileURL,
			SeekableByteChannel channel, int recordLength) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#assignFilePortion(java.lang.String, java.lang.String)
	 */
	@Override
	public FileConstrains assignFilePortion(String componentId, String fileURL, SeekableByteChannel channel, byte[] recordDelimiter) throws IOException {
		return null;
	}


	@Override
	public RunStatus getRunStatus(long runId, List<TrackingEvent> trackingEvents, Long timeout) {
		throw new UnsupportedOperationException("Graph execution status is available only in CloverETL Server environment!");
	}

	@Override
	public List<RunStatus> killGraph(long runId, boolean recursive) {
		throw new UnsupportedOperationException("Graph abortation is available only in CloverETL Server environment!");
	}

	@Override
	public List<RunStatus> killExecutionGroup(String executionGroup, boolean recursive) {
		throw new UnsupportedOperationException("Graph abortation is available only in CloverETL Server environment!");
	}

	@Override
	public List<RunStatus> killChildrenGraphs(boolean recursive) {
		throw new UnsupportedOperationException("Graph abortation is available only in CloverETL Server environment!");
	}

	@Override
	public RunStatus executeGraph(String graphUrl, GraphRuntimeContext runtimeContext) {
		throw new UnsupportedOperationException("This graph execution type is available only in CloverETL Server environment!");
	}

	@Override
	public File newTempFile(String label, String suffix, int allocationHint) throws TempFileCreationException {
		
		try {
			return File.createTempFile(label, suffix == null ? CLOVER_TMP_FILE_SUFFIX : suffix);
		} catch (IOException e) {
			throw new TempFileCreationException(e, label, allocationHint, null, TempSpace.ENGINE_DEFAULT);
		}
	}

	@Override
	public File newTempDir(String label, int allocationHint) throws TempFileCreationException {
		
		/*
		 * TODO as soon as Java 1.7 will be required, use built-in facility.
		 */
		try {
			File tmp = File.createTempFile(label, "");
			if (!tmp.exists()) {
				throw new IOException("Temporary file does no exist: " + tmp.getAbsolutePath());
			}
			if (!tmp.delete()) {
				throw new IOException("Temporary directory could not be created.");
			}
			if (!tmp.mkdir()) {
				throw new IOException("Temporary directory could not be created.");
			}
			return tmp;
		} catch (IOException e) {
			throw new TempFileCreationException(e, label, allocationHint, null, TempSpace.ENGINE_DEFAULT);
		}
	}
	
}
