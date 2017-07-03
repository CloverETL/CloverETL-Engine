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
import java.io.Serializable;
import java.net.URL;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.HttpContextNotAvailableException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.StackTraceWrapperException;
import org.jetel.exception.TempFileCreationException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.Edge;
import org.jetel.graph.EdgeBase;
import org.jetel.graph.GraphParameter;
import org.jetel.graph.IGraphElement;
import org.jetel.graph.JobType;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.dictionary.DictionaryValuesContainer;
import org.jetel.graph.runtime.jmx.GraphTracking;
import org.jetel.graph.runtime.jmx.TrackingEvent;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.FileConstrains;
import org.jetel.util.classloader.GreedyURLClassLoader;
import org.jetel.util.file.WcardPattern;
import org.jetel.util.property.PropertiesUtils;
import org.jetel.util.string.StringUtils;

/**
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jul 11, 2008
 */
public abstract class IAuthorityProxy {

	protected final static Log logger = LogFactory.getLog(IAuthorityProxy.class);
	
	/* This authority proxy is available all the time. */
	private static IAuthorityProxy defaultProxy;
	
	public static IAuthorityProxy getDefaultProxy() {
		if (defaultProxy == null) {
			defaultProxy = AuthorityProxyFactory.createDefaultAuthorityProxy();
		}
		return defaultProxy;
	}

	public static void setDefaultProxy(IAuthorityProxy defaultProxy) {
		IAuthorityProxy.defaultProxy = defaultProxy;
	}

	/**
	 * Returns authority proxy for given graph. If Graph is <code>null</code>, returns default proxy.
	 * 
	 * @param etlGraph
	 * @return authority proxy for given graph. If Graph is <code>null</code>, returns default proxy
	 */
	public static IAuthorityProxy getAuthorityProxy(TransformationGraph graph) {
		return (graph != null) ? graph.getAuthorityProxy() : getDefaultProxy();
	}

	public static class FileOperationResult {
		public enum ProblemType {
			GENERAL_OPERATION_FAILURE, FILE_NOT_FOUND, FILE_UNREADABLE, FILE_UNCHANGEABLE, GENERAL_COPY_ERROR, GENERAL_MOVE_ERROR, GENERAL_DELETE_ERROR, NOT_SANDBOX_URL, NO_PROBLEM
		};

		public final Result result;
		public final ProblemType problemType;
		public final String filePath;
		public final Exception exception;
		public FileOperationResult(final Result result, final ProblemType problemType, String filePath, Exception exception) {
			this.result = result;
			this.problemType = problemType;
			this.filePath = filePath;
			this.exception = exception;
		}
	}
	
	public static class RunStatus implements Serializable {
		
		private static final long serialVersionUID = -4945370081989985664L;
		
		public long runId;
		public String clusterNodeId;
		public String jobUrl;
		public Date startTime;
		public Date endTime;
		public long duration;
		public Result status = Result.N_A;
		public String errException;
		public String errMessage;
		public String errComponent;
		public String errComponentType;
		public Properties graphParameters;
		public DictionaryValuesContainer dictionaryIn;
		public DictionaryValuesContainer dictionaryOut;
		public GraphTracking tracking;
		public JobType jobType;
		public String executionGroup;
		public String executionLabel;
		
		@Override
		public String toString() {
			StringBuilder s = new StringBuilder();
			s.append("Status: ").append(status)
				.append(" RunId: ").append(runId)
				.append(" JobURL: ").append(jobUrl)
				.append(" Message: ").append(errMessage);
			return s.toString();
		}
		
		public Properties toProperties() {
			Properties result = new Properties();
			result.setProperty("runId", Long.toString(runId));
			result.setProperty("clusterNodeId", String.valueOf(clusterNodeId));
			result.setProperty("jobUrl", String.valueOf(jobUrl));
			result.setProperty("startTime", String.valueOf(startTime));
			result.setProperty("endTime", String.valueOf(endTime));
			result.setProperty("duration", Long.toString(duration));
			result.setProperty("status", String.valueOf(status.name()));
			result.setProperty("errException", String.valueOf(errException));
			result.setProperty("errMessage", String.valueOf(errMessage));
			result.setProperty("errComponent", String.valueOf(errComponent));
			result.setProperty("errComponentType", String.valueOf(errComponentType));
			if (dictionaryIn != null) {
				result.setProperty("dictionaryIn", PropertiesUtils.formatProperties(dictionaryIn.toProperties()));
			}
			if (dictionaryOut != null) {
				result.setProperty("dictionaryOut", PropertiesUtils.formatProperties(dictionaryOut.toProperties()));
			}
			//tracking is omitted
			return result;
		}
		
		public RuntimeException getException() {
			if (status.code() < 0 || status == Result.N_A) {
				Exception cause = null;
				if (!StringUtils.isEmpty(errMessage) || !StringUtils.isEmpty(errException)) {
					cause = new StackTraceWrapperException(errMessage, errException); 
				}
				return new JetelRuntimeException(getJobLabel() + " " + jobUrl + (runId > 0 ? ("(#" + runId + ")") : "") + " finished with final status " + status + ".", cause);
			} else {
				return null;
			}
		}

		private String getJobLabel() {
			if (jobType != null) {
				return jobType.getLabel();
			} else {
				return "Job";
			}
		}
		
		/**
		 * Sets {@link #errMessage} and {@link #errException} based on given {@link Exception}.
		 */
		public void setException(Exception e) {
			errMessage = ExceptionUtils.getMessage(null, e);
			errException = ExceptionUtils.stackTraceToString(e);
			
			//try to find caused graph element id
			Throwable t = e;
			while (true) {
				if (t instanceof ConfigurationException) {
					errComponent = ((ConfigurationException) t).getCausedGraphElementId();
				} else if (t instanceof ComponentNotReadyException) {
					IGraphElement graphElement = ((ComponentNotReadyException) t).getGraphElement();
					if (graphElement != null) {
						errComponent = graphElement.getId();
					}
				}
				if (!StringUtils.isEmpty(errComponent) || t.getCause() == null || t == t.getCause()) {
					break;
				}
				t = t.getCause();
			}
		}
	}

	/**
	 * This class represents data target of a remote edge -
	 * pair of output stream and runId of remote job.
	 * @see IAuthorityProxy#getRemoteEdgeDataTarget(String)
	 */
	public static class RemoteEdgeDataTarget {
		private OutputStream outputStream;
		private long dataTargetRunId;
		public RemoteEdgeDataTarget(OutputStream outputStream, long dataTargetRunId) {
			this.outputStream = outputStream;
			this.dataTargetRunId = dataTargetRunId;
		}
		/**
		 * @return outputStream where the data records should be sent
		 */
		public OutputStream getOutputStream() {
			return outputStream;
		}
		/**
		 * @return runId of remote job where the data records are sent
		 */
		public long getDataTargetRunId() {
			return dataTargetRunId;
		}
		@Override
		public String toString() {
			return "RemoteEdgeDataTarget; target="+dataTargetRunId;
		}
	}

	/**
	 * This class represents data source of a remote edge -
	 * pair of input stream and runId of remote job.
	 * @see IAuthorityProxy#getRemoteEdgeDataSource(String)
	 */
	public static class RemoteEdgeDataSource {
		private InputStream inputStream;
		private long dataSourceRunId;
		public RemoteEdgeDataSource(InputStream inputStream, long dataSourceRunId) {
			this.inputStream = inputStream;
			this.dataSourceRunId = dataSourceRunId;
		}
		/**
		 * @return inputStream where the data records are received
		 */
		public InputStream getInputStream() {
			return inputStream;
		}
		/**
		 * @return runId of remote job where the data records are produced
		 */
		public long getDataSourceRunId() {
			return dataSourceRunId;
		}
		@Override
		public String toString() {
			return "RemoteEdgeDataSource; source="+dataSourceRunId;
		}
	}

	/**
	 * Context of the "parent" graph run.
	 * May be null when the AuthorityProxy instance is related to the graph which is not intended to be executed.
	 */
	protected GraphRuntimeContext runtimeContext;

	/**
	 * Sets context for this graph run. Must be called just once for this instance just before graph run.
	 * */
	public void setGraphRuntimeContext(GraphRuntimeContext runtimeContext) {
		this.runtimeContext = runtimeContext;
	}
	
	/**
	 * Executes graph synchronously. 
	 * Thus it waits until the graph is finished or until the timeout expires.
	 * If the graph isn't finished until timeout, it's aborted.
	 * @param graphUrl
	 * @param runtimeContext
	 * @param timeout - If it's null, there is no time limit
	 * @return
	 * @throws InterruptedException if the thread is interrupted while waiting for final event 
	 */
	public abstract RunStatus executeGraphSync(String graphUrl, GraphRuntimeContext runtimeContext, Long timeout) throws InterruptedException;
	
	/**
	 * Executes graph synchronously. 
	 * Thus it waits until the graph is finished or until the timeout expires.
	 * If the graph isn't finished until timeout, it's aborted.
	 * @param graph - ETL transformation graph to execute
	 * @param runtimeContext
	 * @param timeout - If it's null, there is no time limit
	 * @return
	 * @throws InterruptedException if the thread is interrupted while waiting for final event 
	 */
	public abstract RunStatus executeGraphSync(TransformationGraph graph, GraphRuntimeContext runtimeContext, Long timeout) throws InterruptedException;
	
	/**
	 * @param graphUrl - path to graph to execute
	 * @param runtimeContext - this is a part of request to run a graph (graph will be run with as similar runtime context as is possible), 
	 * at least additionalProperties, contextURL, executionGroup and daemon are taken into account
	 * @return RunStatus of running graph
	 * @throws an exception can be thrown if graph cannot be started
	 */
	public abstract RunStatus executeGraph(String graphUrl, GraphRuntimeContext runtimeContext);
	
	public abstract RunStatus executeProfilerJobAsync(String profilerJobUrl, GraphRuntimeContext runtimeContext);
	
	public abstract RunStatus executeProfilerJobSync(String profilerJobUrl, GraphRuntimeContext runtimeContext, Long timeout);
	
	/**
	 * Checks configuration of the given graph, see {@link TransformationGraph#checkConfig(ConfigurationStatus)}.
	 * @param graphUrl URL to checked graph
	 * @param runtimeContext associated runtime context
	 * @return set of configuration problems
	 */
	public abstract ConfigurationStatus checkConfig(String graphUrl, GraphRuntimeContext runtimeContext);
	
	public abstract DataSource getProfilerResultsDataSource();
	
	/**
	 * This method is used for tracking a running graphs. For already finished graphs this method returns
	 * final run status (tracking info included) of the graph.
	 * @param runId run identifier of graph, which is point of interest
	 * @param trackingEvents list of tracking events, which are requested or null if current RunStatus is requested
	 * @param timeout maximum milliseconds to wait for requested tracking events, in case trackingEvents is null,
	 * this parameter is ignored, method is not blocking anyway
	 * @return running information of the graph by specified event
	 * @throws InterruptedException when the thread is waiting for specified events and it's interrupted
	 */
	public abstract RunStatus getRunStatus(long runId, List<TrackingEvent> trackingEvents, Long timeout) throws InterruptedException;
	
	/**
	 * Ask authority to kill job specified by run identifier. It may be any run, not just child run.
	 * @param runId job to kill
	 * @param recursive - true if daemon children jobs should be killed as well
	 * @return List of killed jobs;
	 *  	Specified job must be included in the list, however it's possible that it's not accessible. 
	 *  	E.g. when it's not running any more and it's not persistent. Exception is thrown in such case.
	 *  	List also may contain children jobs killed, but it doesn't contain children jobs finished/failed/killed before.
	 * @throws throws exception when specified job can't be found - e.g. it's already finished and not-persistent  
	 */
	public abstract List<RunStatus> killJob(long runId, boolean recursive);
	
	/**
	 * Ask authority to kill all children jobs belonging to specified execution group.
	 * @param executionGroup name of execution group with jobs to be killed 
	 * @param recursive - true if daemon children jobs should be killed as well
	 * @return final run status for all killed jobs (job finished/failed before this request aren't included)
	 */
	public abstract List<RunStatus> killExecutionGroup(String executionGroup, boolean recursive);

	/**
	 * Ask authority to kill all children jobs.
	 * @param recursive - true if daemon children jobs should be killed as well
	 * @return final run status for all killed jobs (job finished/failed before this request aren't included)
	 */
	public abstract List<RunStatus> killChildrenJobs(boolean recursive);
	
	

	/**
	 * Throws exception if user who executed graph doesn't have write permission for requested sandbox.
	 * Throws exception if requested sandbox isn't accessible (i.e. it's on cluster node which is disconnected).
	 *
	 * @param storageCode
	 * @param path
	 */
	public abstract boolean makeDirectories(String storageCode, String path, boolean makeParents) throws IOException;

	/**
	 * Throws exception if user who executed graph doesn't have write permission for requested sandbox.
	 * Throws exception if requested sandbox isn't accessible (i.e. it's on cluster node which is disconnected).
	 *
	 * @param storageCode
	 * @param path
	 */
	public boolean makeDirectories(String storageCode, String path) throws IOException {
		return makeDirectories(storageCode, path, true);
	};

	/**
	 * Resolves all file names matching the given path eventually containing wildcards.
	 * 
	 * @param sandboxCode
	 *            code of the sandbox
	 * @param wildcardedUrl
	 *            path in sandbox that eventually contains wildcards
	 * @return collection of paths (sandbox-code/path-in-sandbox) to found files
	 * @see WcardPattern
	 */
	public abstract Collection<String> resolveAllFiles(String sandboxCode, String wildcardedPath);

	/**
	 * Throws exception if user who executed graph doesn't have read permission for requested sandbox.
	 * Throws exception if requested sandbox isn't accessible (i.e. it's on cluster node which is disconnected).
	 * 
	 * @param storageCode
	 * @param path
	 * @return
	 */
	public abstract InputStream getSandboxResourceInput(String componentId, String storageCode, String path) throws IOException;

	/**
	 * Returns output stream for updating of specified sandbox resource.
	 * If the specified storage is sandbox with more then one locations, 
	 * it should return stream to the resource, which is in location accessible locally. 
	 * If there is no locally accessible location, it chooses any other location. 
	 * 
	 * @param storageCode
	 * @param path
	 * @param append
	 * 
	 * @return
	 */
	public abstract OutputStream getSandboxResourceOutput(String componentId, String storageCode, String path, boolean append) throws IOException;

//	/**
//	 * Returns true, if this worker instance is "primary" in curretn phase. 
//	 * 
//	 * MZa: will be removed
//	 * 
//	 * @param runId
//	 * @return 
//	 */
//	public abstract boolean isPrimaryWorker();
	
	/**
	 * Takes an edge identifier of a remote edge and returns {@link RemoteEdgeDataSource},
	 * which specified remote source of data records.
	 * This is used by cluster remote edge implementations.
	 */
	public abstract RemoteEdgeDataSource getRemoteEdgeDataSource(String edgeId);

	/**
	 * Takes an edge identifier of a remote edge and returns {@link RemoteEdgeDataTarget},
	 * which specified remote target of data records.
	 * This is used by cluster remote edge implementations.
	 */
	public abstract RemoteEdgeDataTarget getRemoteEdgeDataTarget(String edgeId) throws InterruptedException;

	/**
	 * @param edgeId id of remote edge (edge from RemoteEdgeDataReceiver or edge to RemoteEdgeDataTransmitter) 
	 * @return runId of the opposite side of given remote edge 
	 */
	public abstract long getRemoteEdgeRunId(String edgeId);

	/**
	 * SubgraphInput component uses this method to get {@link EdgeBase} from 
	 * parent graph, which is shared between parent graph and subgraph.
	 * @param inputPortIndex
	 * @return edge from parent graph
	 */
	public abstract Edge getParentGraphSourceEdge(int inputPortIndex);

	/**
	 * SubgraphOutput component uses this method to get {@link EdgeBase} from 
	 * parent graph, which is shared between parent graph and subgraph.
	 * @param inputPortIndex
	 * @return edge from parent graph
	 */
	public abstract Edge getParentGraphTargetEdge(int outputPortIndex);

	/**
	 * Assigns proper portion of a file to current cluster node. It is used mainly by ParallelReader,
	 * which is able to read just pre-defined part of file. Null is returned if the whole file should
	 * be processed. This functionality makes available that each cluster node can process different
	 * part of a single file.
	 * 
	 * @param componentId
	 * @param fileURL
	 * @return
	 * @throws IOException
	 * @see {@link FileConstrains}
	 * @see {@link ParallelReader}
	 */
	public abstract FileConstrains assignFilePortion(String componentId, String fileURL, SeekableByteChannel channel, byte[] recordDelimiter) throws IOException;
	
	/**
	 * Assigns file portion to a cluster. Ensures that the portion start and ends at the record boundary
	 * Used for delimited data by ParallelReader in segment mode. 
	 * @param componentId
	 * @param fileURL
	 * @param channel The channel to be apportioned
	 * @param charset Charset of the channel
	 * @param recordDelimiters An array of delimiters
	 * @return
	 * @throws IOException
	 */
	public abstract FileConstrains assignFilePortion(String componentId, String fileURL, SeekableByteChannel channel, Charset charset, String[] recordDelimiters) throws IOException;

	/**
	 * Assigns file portion to a cluster. Ensures that the portion start and ends at the record boundary
	 * Used for fixed-length data by ParallelReader in segment mode. 
	 * @param componentId
	 * @param fileURL
	 * @param channel The channel to be apportioned
	 * @param recordLength Record length in bytes
	 * @return
	 * @throws IOException
	 */
	public abstract FileConstrains assignFilePortion(String componentId, String fileURL, SeekableByteChannel channel, int recordLength) throws IOException;

	/**
	 * <p>
	 * Provides new temporary file for graph execution. This method and {@link #newTempFile()} are the standard way how
	 * a component gets temporary file for them to be managed in common way. Common management allows cleanup of
	 * obsolete temporary files in cause of e. g. graph failure.
	 * </p>
	 * 
	 * <p>
	 * These methods obsolete {@link File#createTempFile(String, String)} method in whole CloverETL Engine.
	 * </p>
	 * 
	 * <p>
	 * There can be multiple temporary file locations, mainly in server environment. TemporaryFileProvider can choose a
	 * random one or uses given hint to determine the right one. Hint can be used e. g. for cycling over all available
	 * locations to maximize throughput of a component that utilizes massive IO parallelism (e. g. FastSort). More
	 * precisely, if there are <code>n</code> temp. file locations and user specified hint <code>m</code>, location with
	 * index <code>m % n</code> is chosen (<code>%</code> represents modular division).
	 * </p>
	 * 
	 * <p>
	 * If it is not possible to create temporary file in some location (e. g. disk full), next location is chosen.
	 * </p>
	 * 
	 * <p>
	 * Client is able to specify label of temp file. Label is used as a part of file name for better identification (e.
	 * g. component name, temp purpose...).
	 * </p>
	 * 
	 * @param label
	 *            substring of desired temp. file name; may be <code>null</code> if no special label is desired
	 * @param allocationHint
	 *            hint to chose temp. file location if more are present; <code>-1</code> for random allocation
	 * @return created temporary file
	 * @throws TempFileCreationException 
	 */
	public abstract File newTempFile(String label, String suffix, int allocationHint) throws TempFileCreationException;

	public final File newTempFile(String label, int allocationHint) throws TempFileCreationException {
		return newTempFile(label, null, allocationHint);
	}
	
	protected void logNewTempFile(File newTempFile) {
		Node component = ContextProvider.getNode();
		TransformationGraph graph = ContextProvider.getGraph();
		if (component != null && graph != null) {
			logger.trace("Component " + component + " from graph " + graph + " acquires new temporary file " + newTempFile);
		} else if (graph != null) {
			logger.trace("Graph " + graph + " acquires new temporary file " + newTempFile);
		} else {
			logger.trace("New temporary file created " + newTempFile);
		}
	}
	
	/**
	 * Takes given label and adds information about current runId and componentId.
	 * This can be used by newTemp*() methods to create better temporary file name.
	 * The result has following pattern:
	 * <label>_runId_<runId>_componentId_<componentId>
	 */
	protected String decorateTempFileLabel(String label) {
		StringBuilder result = new StringBuilder(label);
		
		//attach runId to file name
		GraphRuntimeContext runtimeContext = ContextProvider.getRuntimeContext();
		if (runtimeContext != null) {
			result.append("_runId_");
			result.append(runtimeContext.getRunId());
		}
		//attach componentId to file name
		String componentId = ContextProvider.getComponentId();
		if (!StringUtils.isEmpty(componentId)) {
			result.append("_componentId_");
			result.append(componentId);
		}
		
		return result.toString();
	}

	/**
	 * Returns new temporary file without custom label being part of its name. If more locations are present, random is
	 * chosen. For more details see {@link #newTempFile(String, int)}.
	 * 
	 * @return created temporary file
	 * @throws TempFileCreationException 
	 */
	public final File newTempFile() throws TempFileCreationException {
		return newTempFile(null, null, -1);
	}
	
	public abstract File newTempDir(String label, int allocationHint) throws TempFileCreationException;
	
	public abstract ClassLoader getClassLoader(URL[] urls, ClassLoader parent, boolean greedy);

	public ClassLoader createClassLoader(URL[] urls, ClassLoader parent, boolean greedy) {
		return createClassLoader(urls, parent, greedy, true);
	}

	/**
	 * Creates new classloader.
	 * @param urls classloader classpath
	 * @param parent parent classloader
	 * @param greedy true if the classloader should be instance of {@link GreedyURLClassLoader}
	 * @param closeOnGraphFinish true if the classloader should be closed on graph finish (true is recommended)
	 * @return
	 */
	public abstract ClassLoader createClassLoader(URL[] urls, ClassLoader parent, boolean greedy, boolean closeOnGraphFinish);

	/**
	 * Creates new classloader with multiple parent classloaders.
	 * @return multi-parent classloader based on the given parent classloaders
	 */
	public abstract ClassLoader createMultiParentClassLoader(ClassLoader... parents);
	
	public abstract boolean isClusterEnabled();
	
	/**
	 * Returns true if runtime environment allows executing partitioned jobs.
	 */
	public abstract boolean isPartitioningEnabled();
	
	/**
	 * Returns secret value of the given parameter or null if no value is available.
	 * An exception can be thrown in case the authority decides the parameter should be resolved
	 * but the requested value is not available or no enough permission for this operation is granted.
	 */
	public String getSecureParamater(String parameterName, String parameterValue) {
		//no secure storage is implemented in default authority proxy
		throw new JetelRuntimeException("Secure parameters are supported only in CloverETL Server environment.");
	}

	public boolean isSecureParameterValue(String parameterValue) {
		return false;
	}
	
	/**
	 * @return meta information about runtime authority
	 */
	public abstract Map<String, String> getAuthorityConfiguration();
	
	/**
	 * Token ID sequence is shared among complete jobflow hierarchy.
	 * For example token created in subgraph/subjobflow must share token ID
	 * sequence to keep token flow monitoring understandable.
	 * 
	 * @return next token ID from parent jobflow
	 */
	public abstract long getNextTokenIdFromParentJob();

	/**
	 * Method implementation may modify specified parameter before it's assigned to the parent graph.
	 * E.g. parameter may be set as secured. 
	 */
	public abstract void modifyGraphParameter(GraphParameter graphParameter);
	
	/**
	 * Returns instance of HttpContext if this job is a RestJob
	 * @return
	 * @throws HttpContextNotAvailableException 
	 */
	public abstract HttpContext getHttpContext() throws HttpContextNotAvailableException;
	
}
