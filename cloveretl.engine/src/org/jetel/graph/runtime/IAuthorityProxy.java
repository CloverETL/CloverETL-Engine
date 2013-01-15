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
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.naming.NamingException;

import org.jetel.data.sequence.Sequence;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationException;
import org.jetel.exception.TempFileCreationException;
import org.jetel.graph.JobType;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.dictionary.DictionaryValuesContainer;
import org.jetel.graph.runtime.jmx.GraphTracking;
import org.jetel.graph.runtime.jmx.TrackingEvent;
import org.jetel.util.FileConstrains;
import org.jetel.util.MiscUtils;
import org.jetel.util.bytes.SeekableByteChannel;
import org.jetel.util.file.WcardPattern;
import org.jetel.util.property.PropertiesUtils;

/**
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jul 11, 2008
 */
public abstract class IAuthorityProxy {
	/* This authority proxy is available all the time. */
	private static IAuthorityProxy defaultProxy;
	
	public static IAuthorityProxy getDefaultProxy() {
		if (defaultProxy == null) {
			defaultProxy = new PrimitiveAuthorityProxy();
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
	
	public static class RunStatus {
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
		
		@Override
		public String toString() {
			StringBuilder s = new StringBuilder();
			s.append("Status: ").append(status)
				.append(" Message: ").append(errMessage)
				.append(" Duration: ").append(duration)
				.append(" RunId: ").append(runId);
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
		
		public String getErrorReport() {
			StringBuilder s = new StringBuilder();
			s.append("Status: ").append(status)
			.append(", Job URL: ").append("\"").append(jobUrl).append("\"")
			.append(", Message: ").append(errMessage != null ? "\""+errMessage+"\"" : "none")
			.append(", Exception: ").append(errException != null ? errException.toString() : "none")
			.append(", Error Component: ").append(errComponent != null ? errComponent : "none")
			.append(", Error Component Type: ").append(errComponentType != null ? errComponentType : "none")
			.append(", Duration: ").append(duration)
			.append(", RunId: ").append(runId);
			return s.toString();
		}

		/**
		 * Sets {@link #errMessage} and {@link #errException} based on given {@link Exception}.
		 */
		public void setException(Exception e) {
			errMessage = MiscUtils.exceptionChainToMessage(null, e);
			errException = MiscUtils.stackTraceToString(e);
			
			//try to find caused graph element id
			Throwable t = e;
			while (true) {
				if (t instanceof ConfigurationException) {
					errComponent = ((ConfigurationException) t).getCausedGraphElementId();
				} else if (t instanceof ComponentNotReadyException) {
					errComponent = ((ComponentNotReadyException) t).getGraphElement().getId();
				} else {
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
	}

	/**
	 * Context of the "parent" graph run.
	 * May be null when the AuthorityProxy instance is related to the graph which is not intended to be executed.
	 */
	protected GraphRuntimeContext runtimeContext;
	
	public abstract Sequence getSharedSequence(Sequence sequence);

	public abstract void freeSharedSequence(Sequence sequence);

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

	@Deprecated
	public abstract Properties getProfilerResultsDatabaseConnectionProperties();
	
	public abstract Connection getProfilerResultsDatabaseConnection() throws MalformedURLException, SQLException, NamingException;
	
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

//	/**
//	 * Called by Cluster Partitioner component on "primary" worker.
//	 * Returns list of output streams to "slave" workers.
//	 * 
//	 * MZa: will be removed
//	 * 
//	 * @param componentId
//	 * @return streams array of size workersCount-1
//	 * @throws IOException
//	 */
//	public abstract OutputStream[] getClusterPartitionerOutputStreams(String componentId) throws IOException;
//	
//	/**
//	 * Called by Cluster Partitioner component on "slave" worker.
//	 * Returns input stream with data from "primary" worker.  
//	 * 
//	 * MZa: will be removed
//	 * 
//	 * @param runId
//	 * @param componentId
//	 * @return 
//	 * @throws IOException
//	 */
//	public abstract InputStream getClusterPartitionerInputStream(String componentId) throws IOException;
//	
//	/**
//	 * Called by ClusterGather component on "primary" worker.
//	 * Returns list of input streams with data from "slave" workers.
//	 * 
//	 * MZa: will be removed
//	 *  
//	 * @param componentId
//	 * @return streams array of size workersCount-1
//	 * @throws IOException
//	 */
//	public abstract InputStream[] getClusterGatherInputStreams(String componentId) throws IOException;
//	
//	/**
//	 * Called by Cluster Gather component on "slave" worker. 
//	 * Returns output stream which will be fed by output data.
//	 * 
//	 * MZa: will be removed 
//	 * 
//	 * @param componentId
//	 * @return
//	 * @throws IOException
//	 */
//	public abstract OutputStream getClusterGatherOutputStream(String componentId) throws IOException;

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
}
