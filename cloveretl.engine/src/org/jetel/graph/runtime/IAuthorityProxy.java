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
import java.util.Properties;

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
public interface IAuthorityProxy {

	public static class RunResult {
		public Result result;
		public String description;
		public long duration;
		public long runId;
	}
		
	public Sequence getSharedSequence(Sequence sequence);

	public void freeSharedSequence(Sequence sequence);

	/**
	 * Executes specified graph. 
	 * 
	 * @param runId - ID of parent run, which calls this method.  
	 * @param graphFileName - path to graph to execute
	 * @param properties - null or instance of properties which can be applied as place-holders in graph XML 
	 * @param logFile - path to file where log output of graph will be saved;
	 * @return
	 */
	public RunResult executeGraph(long runId, String graphFileName, Properties properties, String logFile);

	/**
	 * Throws exception if user who executed graph doesn't have read permission for requested sandbox.
	 * Throws exception if requested sandbox isn't accessible (i.e. it's on cluster node which is disconnected).
	 * @param runId 
	 * 
	 * @param storageCode
	 * @param path
	 * @return
	 */
	public InputStream getSandboxResourceInput(long runId, String storageCode, String path) throws IOException;

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
	public OutputStream getSandboxResourceOutput(long runId, String storageCode, String path) throws IOException;

	/**
	 * Returns true, if this worker instance is "primary" in curretn phase. 
	 * 
	 * MZa: will be removed
	 * 
	 * @param runId
	 * @return 
	 */
	public boolean isPrimaryWorker(long runId);
	
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
	public OutputStream[] getClusterPartitionerOutputStreams(long runId, String componentId) throws IOException;
	
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
	public InputStream getClusterPartitionerInputStream(long runId, String componentId) throws IOException;
	
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
	public InputStream[] getClusterGatherInputStreams(long runId, String componentId) throws IOException;
	
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
	public OutputStream getClusterGatherOutputStream(long runId, String componentId) throws IOException;

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
	public FileConstrains assignFilePortion(long runId, String componentId, String fileURL, SeekableByteChannel channel, byte[] recordDelimiter) throws IOException;
	
}
