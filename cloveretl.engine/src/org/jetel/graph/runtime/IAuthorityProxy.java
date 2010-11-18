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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

import org.jetel.data.sequence.Sequence;
import org.jetel.graph.Result;
import org.jetel.util.FileConstrains;
import org.jetel.util.bytes.SeekableByteChannel;

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

	public static class RunResult {
		public Result result;
		public String description;
		public long duration;
		public long runId;
	}
		
	public abstract Sequence getSharedSequence(Sequence sequence);

	public abstract void freeSharedSequence(Sequence sequence);

	/**
	 * Executes specified graph. 
	 * 
	 * @param runId - ID of parent run, which calls this method.  
	 * @param graphFileName - path to graph to execute
	 * @param runtimeContext - this is a part of request to run a graph (graph will be run with as similar runtime context as is possible), 
	 * at least additionalProperties are taken into account, also contextURL should be correctly predefined
	 * @param logFile - path to file where log output of graph will be saved;
	 * @return
	 */
	public abstract RunResult executeGraph(long runId, String graphFileName, GraphRuntimeContext runtimeContext, String logFile);

	/**
	 * Throws exception if user who executed graph doesn't have read permission for requested sandbox.
	 * Throws exception if requested sandbox isn't accessible (i.e. it's on cluster node which is disconnected).
	 * @param runId 
	 * 
	 * @param storageCode
	 * @param path
	 * @return
	 */
	public abstract InputStream getSandboxResourceInput(long runId, String storageCode, String path) throws IOException;

	/**
	 * Returns output stream for updating of specified sandbox resource.
	 * If the specified storage is sandbox with more then one locations, 
	 * it should return stream to the resource, which is in location accessible locally. 
	 * If there is no locally accessible location, it chooses any other location. 
	 * 
	 * @param runId
	 * @param storageCode
	 * @param path
	 * @return
	 */
	public abstract OutputStream getSandboxResourceOutput(long runId, String storageCode, String path) throws IOException;

	/**
	 * Returns true, if this worker instance is "primary" in curretn phase. 
	 * 
	 * MZa: will be removed
	 * 
	 * @param runId
	 * @return 
	 */
	public abstract boolean isPrimaryWorker(long runId);
	
	/**
	 * Called by Cluster Partitioner component on "primary" worker.
	 * Returns list of output streams to "slave" workers.
	 * 
	 * MZa: will be removed
	 * 
	 * @param runId
	 * @param componentId
	 * @return streams array of size workersCount-1
	 * @throws IOException
	 */
	public abstract OutputStream[] getClusterPartitionerOutputStreams(long runId, String componentId) throws IOException;
	
	/**
	 * Called by Cluster Partitioner component on "slave" worker.
	 * Returns input stream with data from "primary" worker.  
	 * 
	 * MZa: will be removed
	 * 
	 * @param runId
	 * @param componentId
	 * @return 
	 * @throws IOException
	 */
	public abstract InputStream getClusterPartitionerInputStream(long runId, String componentId) throws IOException;
	
	/**
	 * Called by ClusterGather component on "primary" worker.
	 * Returns list of input streams with data from "slave" workers.
	 * 
	 * MZa: will be removed
	 *  
	 * @param runId
	 * @param componentId
	 * @return streams array of size workersCount-1
	 * @throws IOException
	 */
	public abstract InputStream[] getClusterGatherInputStreams(long runId, String componentId) throws IOException;
	
	/**
	 * Called by Cluster Gather component on "slave" worker. 
	 * Returns output stream which will be fed by output data.
	 * 
	 * MZa: will be removed 
	 * 
	 * @param runId
	 * @param componentId
	 * @return
	 * @throws IOException
	 */
	public abstract OutputStream getClusterGatherOutputStream(long runId, String componentId) throws IOException;

	/**
	 * Assigns proper portion of a file to current cluster node. It is used mainly by ParallelReader,
	 * which is able to read just pre-defined part of file. Null is returned if the whole file should
	 * be processed. This functionality makes available that each cluster node can process different
	 * part of a single file.
	 * 
	 * @param runId
	 * @param componentId
	 * @param fileURL
	 * @return
	 * @throws IOException
	 * @see {@link FileConstrains}
	 * @see {@link ParallelReader}
	 */
	public abstract FileConstrains assignFilePortion(long runId, String componentId, String fileURL, SeekableByteChannel channel, byte[] recordDelimiter) throws IOException;
	
	/**
	 * Assigns file portion to a cluster. Ensures that the portion start and ends at the record boundary
	 * Used for delimited data by ParallelReader in segment mode. 
	 * @param runId
	 * @param componentId
	 * @param fileURL
	 * @param channel The channel to be apportioned
	 * @param charset Charset of the channel
	 * @param recordDelimiters An array of delimiters
	 * @return
	 * @throws IOException
	 */
	public abstract FileConstrains assignFilePortion(long runId, String componentId, String fileURL, SeekableByteChannel channel, Charset charset, String[] recordDelimiters) throws IOException;

	/**
	 * Assigns file portion to a cluster. Ensures that the portion start and ends at the record boundary
	 * Used for fixed-length data by ParallelReader in segment mode. 
	 * @param runId
	 * @param componentId
	 * @param fileURL
	 * @param channel The channel to be apportioned
	 * @param recordLength Record length in bytes
	 * @return
	 * @throws IOException
	 */
	public abstract FileConstrains assignFilePortion(long runId, String componentId, String fileURL, SeekableByteChannel channel, int recordLength) throws IOException;

}
