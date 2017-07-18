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
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.sql.DataSource;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.HttpContextNotAvailableException;
import org.jetel.exception.TempFileCreationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.Edge;
import org.jetel.graph.GraphParameter;
import org.jetel.graph.IGraphElement;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.graph.dictionary.DictionaryValuesContainer;
import org.jetel.graph.runtime.jmx.TrackingEvent;
import org.jetel.main.runGraph;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.FileConstrains;
import org.jetel.util.classloader.GreedyURLClassLoader;
import org.jetel.util.classloader.MultiParentClassLoader;
import org.jetel.util.file.FileUtils;

/**
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jul 11, 2008
 */
public class PrimitiveAuthorityProxy extends IAuthorityProxy {

	/**
	 * Auto-incremented number, which is used in {@link #getUniqueRunId()}
	 * for generating unique run IDs. 
	 */
	private static long runIdSequence = 1;

	/**
	 * Suffix of temp files created by standalone engine
	 */
	private static final String CLOVER_TMP_FILE_SUFFIX = ".clover.tmp";

	public PrimitiveAuthorityProxy(){
		super();
	}
	
	protected GraphRuntimeContext prepareRuntimeContext(GraphRuntimeContext givenRuntimeContext, long runId) {
        GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
        runtimeContext.setRunId(runId);
        runtimeContext.setParentRunId(givenRuntimeContext.getRunId());
        runtimeContext.setLogLevel(Level.ALL);
        runtimeContext.setLogLocation(givenRuntimeContext.getLogLocation());
        if (runtimeContext.getLogLocation() != null) {
        	prepareLogger(runtimeContext);
        }
        runtimeContext.setAdditionalProperties(givenRuntimeContext.getAdditionalProperties());
        runtimeContext.setContextURL(givenRuntimeContext.getContextURL());
        runtimeContext.setDictionaryContent(givenRuntimeContext.getDictionaryContent());
        runtimeContext.setRuntimeClassPath(givenRuntimeContext.getRuntimeClassPath());
        runtimeContext.setCompileClassPath(givenRuntimeContext.getCompileClassPath());
        runtimeContext.setJobUrl(givenRuntimeContext.getJobUrl());
        
        // debug mode has to be turned off, parallel edge debugging is not available for non-server graph processing 
        runtimeContext.setEdgeDebugging(false);
        
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
    			rr.errMessage = ExceptionUtils.getMessage("Error during graph initialization.", e);           
            	rr.errException = ExceptionUtils.stackTraceToString(e);
            	rr.status = Result.ERROR;
            	return rr;
            } catch (RuntimeException e) {
            	rr.endTime = new Date(System.currentTimeMillis());
            	rr.duration = rr.endTime.getTime() - rr.startTime.getTime(); 
            	rr.errMessage = ExceptionUtils.getMessage("Error during graph initialization.", e);           
            	rr.errException = ExceptionUtils.stackTraceToString(e);
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
    			rr.errMessage = ExceptionUtils.getMessage("Graph was unexpectedly interrupted !", e);            
            	rr.errException = ExceptionUtils.stackTraceToString(e);
            	rr.status = Result.ERROR;
            	return rr;
    		} catch (ExecutionException e) {
            	rr.endTime = new Date(System.currentTimeMillis());
            	rr.duration = rr.endTime.getTime() - rr.startTime.getTime(); 
    			rr.errMessage = ExceptionUtils.getMessage("Error during graph processing !", e);            
            	rr.errException = ExceptionUtils.stackTraceToString(e);
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
        	rr.errException = ExceptionUtils.stackTraceToString(graph.getWatchDog().getCauseException());
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
	 * @throws InterruptedException 
	 * 
	 * @see org.jetel.graph.runtime.IAuthorityProxy#executeGraph(long, java.lang.String)
	 */
	@Override
	public RunStatus executeGraphSync(String graphFileName, GraphRuntimeContext givenRuntimeContext, Long timeout) throws InterruptedException {
		RunStatus rr = new RunStatus();
        long runId = (this.runtimeContext == null) ? 0:this.runtimeContext.getRunId();
        
		InputStream in = null;		

		long startTime = System.currentTimeMillis();
		rr.startTime = new Date(startTime);
		
		try {
            in = FileUtils.getInputStream(givenRuntimeContext.getContextURL(), graphFileName);
        } catch (IOException e) {
        	rr.endTime = new Date(System.currentTimeMillis());
        	rr.duration = rr.endTime.getTime() - rr.startTime.getTime(); 
        	rr.errMessage = ExceptionUtils.getMessage("Error - graph definition file can't be read!", e);
        	rr.errException = ExceptionUtils.stackTraceToString(e);
        	rr.status = Result.ERROR;
        	return rr;
        }
		
		GraphRuntimeContext runtimeContext = prepareRuntimeContext(givenRuntimeContext, rr.runId = getUniqueRunId());
        runtimeContext.setUseJMX(givenRuntimeContext.useJMX());
        
        TransformationGraph graph = null;
		try {
			graph = TransformationGraphXMLReaderWriter.loadGraph(in, runtimeContext);
			rr.jobUrl = graphFileName;
        } catch (XMLConfigurationException e) {
        	rr.endTime = new Date(System.currentTimeMillis());
        	rr.duration = rr.endTime.getTime() - rr.startTime.getTime(); 
        	rr.errMessage = ExceptionUtils.getMessage("Error in reading graph from XML!", e);
        	rr.errException = ExceptionUtils.stackTraceToString(e);
        	rr.status = Result.ERROR;
        	return rr;
        } catch (GraphConfigurationException e) {
        	rr.endTime = new Date(System.currentTimeMillis());
        	rr.duration = rr.endTime.getTime() - rr.startTime.getTime(); 
        	rr.errMessage = ExceptionUtils.getMessage("Error - graph's configuration invalid!", e);
        	rr.errException = ExceptionUtils.stackTraceToString(e);
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

		GraphRuntimeContext runtimeContext = prepareRuntimeContext(givenRuntimeContext, rr.runId = getUniqueRunId());

		return executeGraphSync(rr, graph, runtimeContext, timeout);
	}

	/**
	 * @return unique long number, which can be used as run ID of newly executed graphs
	 */
	public static synchronized long getUniqueRunId() {
		return runIdSequence++;
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
			String logLocation = FileUtils.getFile(ContextProvider.getContextURL(), runtimeContext.getLogLocation());
			logAppender = new FileAppender(new PatternLayout("%d %-5p %-3X{runId} [%t] %m%n"), logLocation);
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
	public InputStream getSandboxResourceInput(String componentId, String storageCode, String path) throws IOException {
		throw new UnsupportedOperationException("Sandbox resources are accessible only in CloverETL Server environment!");
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#getSandboxResourceOutput(java.lang.String, java.lang.String)
	 */
	@Override
	public OutputStream getSandboxResourceOutput(String componentId, String storageCode, String path, boolean append) throws IOException {
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
	public RunStatus getRunStatus(long runId, List<TrackingEvent> trackingEvents, Long timeout) throws InterruptedException {
		throw new UnsupportedOperationException("Graph execution status is available only in CloverETL Server environment!");
	}

	@Override
	public List<RunStatus> killJob(long runId, boolean recursive) {
		throw new UnsupportedOperationException("Graph abortation is available only in CloverETL Server environment!");
	}

	@Override
	public List<RunStatus> killExecutionGroup(String executionGroup, boolean recursive) {
		throw new UnsupportedOperationException("Graph abortation is available only in CloverETL Server environment!");
	}

	@Override
	public List<RunStatus> killChildrenJobs(boolean recursive) {
		throw new UnsupportedOperationException("Graph abortation is available only in CloverETL Server environment!");
	}

	@Override
	public RunStatus executeGraph(String graphUrl, GraphRuntimeContext runtimeContext) {
		throw new UnsupportedOperationException("Subgraph execution is available only in CloverETL Server environment!");
	}

	@Override
	public ConfigurationStatus checkConfig(String graphUrl, GraphRuntimeContext runtimeContext) {
		throw new UnsupportedOperationException("Subgraph configuration check is available only in CloverETL Server environment!");
	}
	
	@Override
	public File newTempFile(String label, String suffix, int allocationHint) throws TempFileCreationException {
		try {
			label = decorateTempFileLabel(label);
			File file = File.createTempFile(label, suffix == null ? CLOVER_TMP_FILE_SUFFIX : suffix);
			logNewTempFile(file);
			return file;
		} catch (IOException e) {
			throw new TempFileCreationException(e, label, allocationHint, null);
		}
	}

	@Override
	public File newTempDir(String label, int allocationHint) throws TempFileCreationException {
		try {
			label = decorateTempFileLabel(label);
			File tmp = Files.createTempDirectory(label).toFile(); // CLO-226
			logNewTempFile(tmp);
			return tmp;
		} catch (IOException e) {
			throw new TempFileCreationException(e, label, allocationHint, null);
		}
	}
	
	@Override
	public RunStatus executeProfilerJobAsync(String profilerJobUrl, GraphRuntimeContext runtimeContext) {
		throw new UnsupportedOperationException("Profiler job execution is available only in CloverETL Server environment");
	}
	
	@Override
	public DataSource getProfilerResultsDataSource() {
		throw new UnsupportedOperationException("Profiler results storage is available only in CloverETL Server environment");
	}
	
	@Override
	public RemoteEdgeDataSource getRemoteEdgeDataSource(String edgeId) {
		throw new UnsupportedOperationException("remote edges are not available for local graphs");
	}

	@Override
	public RemoteEdgeDataTarget getRemoteEdgeDataTarget(String edgeId) throws InterruptedException {
		throw new UnsupportedOperationException("remote edges are not available for local graphs");
	}

	@Override
	public long getRemoteEdgeRunId(String edgeId) {
		throw new UnsupportedOperationException("remote edges are not available for local graphs");
	}
	
	@Override
	public RunStatus executeProfilerJobSync(String profilerJobUrl, GraphRuntimeContext runtimeContext, Long timeout) {
		throw new UnsupportedOperationException("Profiler job execution is available only in CloverETL Server environment");
	}

	@Override
	public ClassLoader getClassLoader(URL[] urls, ClassLoader parent, boolean greedy) {
		return createClassLoader(urls, parent, greedy);
	}

	@Override
	public ClassLoader createClassLoader(final URL[] urls, ClassLoader parent, final boolean greedy, boolean closeOnGraphFinish) {
		if (parent == null) {
			parent = PrimitiveAuthorityProxy.class.getClassLoader();
		}
        if (urls == null || urls.length == 0) {
        	return parent;
        } else {
        	final ClassLoader parentFinal = parent; 
        	return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
                @Override
				public ClassLoader run() {
                	if (greedy) {
                		return new GreedyURLClassLoader(urls, parentFinal);
                	} else {
                		return new URLClassLoader(urls, parentFinal);
                	}
                }
        	});
        }
	}
	
	@Override
	public ClassLoader createMultiParentClassLoader(ClassLoader... parents) {
		return new MultiParentClassLoader(parents);
	}
	
	@Override
	public boolean isClusterEnabled() {
		return false;
	}

	@Override
	public boolean isPartitioningEnabled() {
		return false;
	}

	@Override
	public Edge getParentGraphSourceEdge(int inputPortIndex) {
		throw new UnsupportedOperationException("subgraphs are not avaible in standalone engine");
	}

	@Override
	public Edge getParentGraphTargetEdge(int outputPortIndex) {
		throw new UnsupportedOperationException("subgraphs are not avaible in standalone engine");
	}

	@Override
	public Map<String, String> getAuthorityConfiguration() {
		return Collections.emptyMap();
	}
	
	@Override
	public long getNextTokenIdFromParentJob() {
		throw new UnsupportedOperationException("token ID sequence is not available for local execution");
	}

	@Override
	public void modifyGraphParameter(GraphParameter graphParameter) {
		// no params modifications
	}

	@Override
	public HttpContext getHttpContext() throws HttpContextNotAvailableException {
		throw new HttpContextNotAvailableException("HTTP Context is not available for local execution");
	}
	
}
