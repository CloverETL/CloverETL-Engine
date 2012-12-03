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
package org.jetel.component.hadooploader;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.jetel.component.hadooploader.HadoopCloverConvert.Clover2Hadoop;
import org.jetel.data.DataRecord;
import org.jetel.database.IConnection;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.TransformationGraph;
import org.jetel.hadoop.component.IHadoopSequenceFileFormatter;
import org.jetel.hadoop.connection.HadoopConnection;
import org.jetel.hadoop.connection.HadoopURLUtils;
import org.jetel.hadoop.connection.IHadoopConnection;
import org.jetel.metadata.DataRecordMetadata;


public class HadoopSequenceFileFormatter implements
		IHadoopSequenceFileFormatter {

	private FileSystem dfs;
	private SequenceFile.Writer writer;
	private int keyField;
	private int valueField;
	private String keyFieldName;
	private String valueFieldName;
	private Clover2Hadoop keyCopy;
	private Clover2Hadoop valCopy;
	private TransformationGraph graph;
	
	
	public HadoopSequenceFileFormatter(String keyFieldName, String valueFieldName) {
		this.keyFieldName=keyFieldName;
		this.valueFieldName=valueFieldName;
	}
	
	@Override
	public void init(DataRecordMetadata _metadata)
			throws ComponentNotReadyException {
		if (_metadata.getNumFields()<2){
			throw new ComponentNotReadyException("Hadoop formatter needs metadata with at least 2 fields.");
		}
		if (keyFieldName==null){
			keyField=0;
			valueField=1;
		}else{
			keyField = _metadata.getFieldPosition(keyFieldName);
			valueField = _metadata.getFieldPosition(valueFieldName);
			
			if (keyField == -1){
				throw new ComponentNotReadyException("Can't find key field of name \""+keyFieldName+"\" in metadata.");
			}
			if (valueField == -1){
				throw new ComponentNotReadyException("Can't find value field of name \""+keyFieldName+"\" in metadata.");
			}
		}
		try{
			keyCopy=HadoopCloverConvert.getC2HCopier(_metadata.getField(keyField));
			valCopy=HadoopCloverConvert.getC2HCopier(_metadata.getField(valueField));
		}catch(IOException ex){
			throw new ComponentNotReadyException(ex);
		}

	}

	@Override
	public void reset() {
		dfs = null; // causes DFS to be recreated from fresh connection
	}

	@Override
	public void setDataTarget(Object outputDataTarget) throws IOException {
		FileSystem tmpDFS;
		
		if (outputDataTarget instanceof SequenceFile.Writer) {
			writer = (SequenceFile.Writer) outputDataTarget;
			return;
		}

		if (outputDataTarget instanceof URI) {
			if (!HadoopURLUtils.isHDFSUri((URI)outputDataTarget))
				throw new IOException("Not a valid HDFS/Hadoop URL - "+outputDataTarget);
			
			final String connectionName = ((URI) outputDataTarget).getHost();

			if(graph==null) graph = ContextProvider.getGraph();
			if (graph == null) {
				throw new IOException(
						String.format(
								"Internal error: Cannot find HDFS connection [%s] referenced in fileURL \"%s\". Missing TransformationGraph instance.",
								connectionName, outputDataTarget));
			}

			ClassLoader formerContextClassloader = Thread.currentThread()
					.getContextClassLoader();
			Thread.currentThread().setContextClassLoader(
					this.getClass().getClassLoader());
			try {

				if(dfs==null){
					IConnection conn = graph.getConnection(connectionName);
					if (conn == null)
						throw new IOException(
								String.format(
										"Cannot find HDFS connection [%s] referenced in fileURL \"%s\".",
										connectionName, outputDataTarget));
					if (!(conn instanceof HadoopConnection)) {
						throw new IOException(String.format(
								"Connection [%s:%s] is not of HDFS type.",
								conn.getId(), conn.getName()));
					}
					conn.init(); // try to init - in case it was not already initialized
					tmpDFS=(FileSystem) ((HadoopConnection) conn)
							.getConnection().getDFS();

				}else{
					tmpDFS=dfs;
				}
				writer = SequenceFile.createWriter(tmpDFS, // FileSystem
						new Configuration(), // Configuration
						new Path(((URI) outputDataTarget).getPath()), // Path to new file in HDFS
						keyCopy.getValueClass(), // Key Data Type
						valCopy.getValueClass(), // Value Data Type
						SequenceFile.CompressionType.NONE);
			} catch(ComponentNotReadyException ex){
				throw new IOException(ex);
			} finally {
				Thread.currentThread().setContextClassLoader(
						formerContextClassloader);
			}

		} else {
			throw new IOException("Unsupported data target type: "
					+ outputDataTarget.getClass().getName());
		}

	}

	@Override
	public void close() throws IOException {
		if (writer != null) {
			writer.close();
		}
	}

	@Override
	public int write(DataRecord record) throws IOException {
		 keyCopy.setValue(record.getField(keyField));
		 valCopy.setValue(record.getField(valueField));
		 writer.append(keyCopy.getValue(), valCopy.getValue());
		 return 1;
	}

	@Override
	public int writeHeader() throws IOException {
		// no header
		return 0;
	}

	@Override
	public int writeFooter() throws IOException {
		// no footer
		return 0;
	}

	@Override
	public void flush() throws IOException {
		writer.syncFs();
	}

	@Override
	public void finish() throws IOException {
		writer.syncFs();
	}

	@Override
	public boolean isURITargetPreferred() {
		return true;
	}

	@Override
	public void setAppend(boolean append) {
		// TODO Auto-generated method stub

	}
	
	

	@Override
	public void setKeyValueFields(String keyFieldName, String valueFieldName) {
		this.keyFieldName=keyFieldName;
		this.valueFieldName=valueFieldName;
		
	}

	@Override
	public void setHadoopConnection(IHadoopConnection conn) {
		this.dfs = (FileSystem)conn.getDFS();
	}

	@Override
	public void setGraph(TransformationGraph graph) {
		this.graph=graph;
	}
	
}
