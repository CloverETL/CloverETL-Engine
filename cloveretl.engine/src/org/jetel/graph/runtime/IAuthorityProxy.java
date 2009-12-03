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
	 * 
	 * @param runId
	 * @param storageCode
	 * @param path
	 * @return
	 */
	public OutputStream getSandboxResourceOutput(long runId, String storageCode, String path) throws IOException;
	
	/**
	 * Provides list of input streams for all parts of given file in a partitioned sandbox.
	 * @param runId
	 * @param storageCode
	 * @param path
	 * @return
	 */
	//public InputStream[] getPartitionedSandboxResourceInput(long runId, String storageCode, String path) throws IOException;

	/**
	 * Provides list of output streams for all parts of given file in a partitioned sandbox.
	 * @param runId
	 * @param storageCode
	 * @param path
	 * @return
	 */
	//public OutputStream[] getPartitionedSandboxResourceOutput(long runId, String storageCode, String path) throws IOException;

	//will be removed
	public boolean isPrimaryWorker(long runId);
	//will be removed
	public OutputStream[] getClusterPartitionerOutputStreams(long runId, String componentId) throws IOException;
	//will be removed
	public InputStream getClusterPartitionerInputStream(long runId, String componentId) throws IOException;
	//will be removed
	public InputStream[] getClusterGatherInputStreams(long runId, String componentId) throws IOException;
	//will be removed
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
