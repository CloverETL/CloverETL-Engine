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
package org.jetel.hadoop.provider.filesystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.parser.AbstractParser;
import org.jetel.database.IConnection;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.TransformationGraph;
import org.jetel.hadoop.component.IHadoopSequenceFileParser;
import org.jetel.hadoop.connection.HadoopConnection;
import org.jetel.hadoop.connection.HadoopURLUtils;
import org.jetel.hadoop.provider.filesystem.HadoopCloverConvert.Hadoop2Clover;
import org.jetel.hadoop.service.filesystem.HadoopFileSystemService;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.SandboxUrlUtils;

public class HadoopSequenceFileParser extends AbstractParser implements IHadoopSequenceFileParser {

	private static final String SANDBOX_TO_FILE_URL_ERROR = "Failed to convert %s to a local file URL";

	private FileSystem fs;
	private String user;
	private Configuration config;
	private SequenceFile.Reader reader;
	private int keyField;
	private int valueField;
	private String keyFieldName;
	private String valueFieldName;
	private Writable keyValue;
	private Writable dataValue;
	private Hadoop2Clover keyCopy;
	private Hadoop2Clover valCopy;
	private DataRecordMetadata metadata;
	private TransformationGraph graph;
	
	private IParserExceptionHandler exceptionHandler;
	
	private static Logger logger = Logger.getLogger(HadoopSequenceFileParser.class);
	
	
	public HadoopSequenceFileParser(DataRecordMetadata metadata, String keyFieldName, String valueFieldName, String user, Configuration config) {
		this.metadata = metadata;
		this.keyFieldName = keyFieldName;
		this.valueFieldName = valueFieldName;
		this.user = user;
		this.config = config;
	}
	
	
	@Override
	public DataRecord getNext() throws JetelException {
		// create a new data record
		DataRecord record = DataRecordFactory.newRecord(metadata);
		record.init();
		return getNext(record);
	}

	@Override
	public int skip(int nRec) throws JetelException {
		for (int i = 0; i < nRec; i++) {
			try {
				reader.next(keyValue, dataValue);
			} catch (IOException e) {
				throw new JetelException("Error when skipping record.", e);
			}
		}
		return nRec;
	}

	@Override
	public void init() throws ComponentNotReadyException {
		if (metadata == null) {
			throw new ComponentNotReadyException("No metadata defined");
		}

		if (keyFieldName == null) {
			keyField = 0;
			valueField = 1;
		} else {
			keyField = metadata.getFieldPosition(keyFieldName);
			valueField = metadata.getFieldPosition(valueFieldName);

			if (keyField == -1) {
				throw new ComponentNotReadyException(
						"Can't find key field of name \"" + keyFieldName + "\" in metadata.");
			}
			if (valueField == -1) {
				throw new ComponentNotReadyException("Can't find value field of name \"" + keyFieldName + "\" in metadata.");
			}
		}
	}

	private void initCopyObjects() throws ComponentNotReadyException {
		if (reader == null) {
			throw new ComponentNotReadyException("No source data reader defined.");
		}
		
		DataFieldType hKeyType;
		DataFieldType hValueType;
		try {
			hKeyType = HadoopCloverConvert.hadoopType2Clover(reader.getKeyClass());
			hValueType = HadoopCloverConvert.hadoopType2Clover(reader.getValueClass());
			keyCopy = HadoopCloverConvert.getH2CCopier(reader.getKeyClass());
			valCopy = HadoopCloverConvert.getH2CCopier(reader.getValueClass());
			keyValue = (Writable) reader.getKeyClass().newInstance();
			dataValue = (Writable) reader.getValueClass().newInstance();
		} catch (Exception ex) {
			throw new ComponentNotReadyException("Error when initializing HadoopSequenceFile parser.", ex);
		}
		
		if (metadata.getField(keyField).getDataType() != hKeyType)
			throw new ComponentNotReadyException(String.format("Incompatible Clover & Hadoop data types for Key \"%s\" (%s <> %s/%s).",
					metadata.getField(keyField).getName(), metadata.getField(keyField).getDataType(), reader.getKeyClassName(), hKeyType));
		
		if (metadata.getField(valueField).getDataType() != hValueType)
			throw new ComponentNotReadyException(String.format("Incompatible Clover & Hadoop data types for Value \"%s\" (%s <> %s/%s).",
					metadata.getField(valueField).getName(), metadata.getField(keyField).getDataType(), reader.getValueClassName(), hValueType));
	}
	
	
	@Override
	public void setDataSource(Object inputDataSource) throws ComponentNotReadyException {
		if (releaseDataSource) {
			releaseDataSource();
		}
		if (inputDataSource instanceof SequenceFile.Reader) {
			reader = (SequenceFile.Reader) inputDataSource;
			return;
		}

		if (inputDataSource instanceof URI) {
			URI uri = (URI) inputDataSource;
			
			try {
				uri = sandboxToFileURI(uri);
			} catch (IOException e) {
				throw new ComponentNotReadyException(e);
			}
			
			ClassLoader formerContextClassloader = Thread.currentThread().getContextClassLoader();
			Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
			try {
				FileSystem fileSystem = fs != null ? fs : getFileSystem(uri, graph, user, config);
				reader = new SequenceFile.Reader(fileSystem, new Path(uri.getPath()), config);
			} catch (IOException e) {
				throw new ComponentNotReadyException("Failed to create Hadoop sequence file reader", e);
			} finally {
				Thread.currentThread().setContextClassLoader(formerContextClassloader);
			}

		} else {
			throw new ComponentNotReadyException("Unsupported data source type: " + inputDataSource.getClass().getName());
		}
		initCopyObjects();
	}


	static URI sandboxToFileURI(URI uri) throws IOException {
		if (SandboxUrlUtils.isSandboxUri(uri)) {
			URL url = SandboxUrlUtils.toLocalFileUrl(FileUtils.getFileURL(uri.toString()));
			if (url == null) {
				throw new IOException(String.format(SANDBOX_TO_FILE_URL_ERROR, uri));
			}
			try {
				return url.toURI();
			} catch (URISyntaxException e) {
				throw new IOException(String.format(SANDBOX_TO_FILE_URL_ERROR, uri), e);
			}
		}
		return uri;
	}

	/**
	 * Provides Hadoop FileSystem based on specified URI:
	 *  - if the URI has "hdfs" scheme and its authority part is a connection ID of a Hadoop connection present in specified graph,
	 *    returned FileSystem if HDFS {@link DistributedFileSystem} of NameNode defined in that connection.
	 *  - otherwise Hadoop factory method FileSystem.get(uri, configuration) gets the job done.
	 */
	static FileSystem getFileSystem(URI uri, TransformationGraph graph, String user, Configuration configuration) throws ComponentNotReadyException, IOException {
		FileSystem fileSystem = null;
		if (HadoopURLUtils.isHDFSUri(uri)) {
			final String connectionName = uri.getAuthority();

			if (graph == null) {
				graph = ContextProvider.getGraph();
			}
			if (graph != null) {
				IConnection conn = graph.getConnection(connectionName);
				if (conn == null || !(conn instanceof HadoopConnection)) {
					if (uri.getHost() != null) {
						logger.debug("Hadoop connection with ID \"" + connectionName + "\" not found; assuming it's a host name. Processed URI: " + uri);
					} else {
						throw new ComponentNotReadyException("Invalid HDFS URI: " + uri + ". Reason: '" + connectionName + "' is neither ID of existing Hadoop connection nor valid host name");
					}
				}
				if (conn != null && conn instanceof HadoopConnection) {
					conn.init(); // try to init - in case it was not already initialized
					try {
						fileSystem = (FileSystem) ((HadoopConnection) conn).getFileSystemService().getDFS();
					} catch (IOException e) {
						throw new ComponentNotReadyException("Failed to access file system of Hadoop connection with ID \'" + connectionName + "'", e);
					}
				}
			} else {
				logger.debug("Missing TransformationGraph instance -> cannot decide whether hdfs URI contains connection ID");
			}
		}
		
		if (fileSystem == null) {
			// Here it's assumed that if MultiFileReader detects that a local file is to be read, it always
			// provides to this method an absolute URI (resolved in context of contextURL) with scheme "file" (never null).
			try {
				fileSystem = FileSystem.get(uri, configuration, user);
			} catch (InterruptedException e) {
				throw new IOException("Internal error: failed to retrieve file system", e);
			}
		}
		return fileSystem;
	}

	@Override
	protected void releaseDataSource() {
		try {
			close();
		} catch (IOException e) {
			logger.warn("Failed to release data source", e);
		}
	}

	@Override
	public void close() throws IOException {
		if (reader != null) {
			reader.close();
		}
	}

	@Override
	public DataRecord getNext(DataRecord record) throws JetelException {
		try {
			if (!reader.next(keyValue, dataValue)) {
				return null;
			}
		} catch (IOException e) {
			throw new JetelException("Error when reading data record.", e);
		}
		this.keyCopy.copyValue(keyValue, record.getField(keyField));
		this.valCopy.copyValue(dataValue, record.getField(valueField));
		
		return record;
	}

	@Override
	public void setExceptionHandler(IParserExceptionHandler handler) {
		this.exceptionHandler = handler;
	}

	@Override
	public IParserExceptionHandler getExceptionHandler() {
		return this.exceptionHandler;
	}

	@Override
	public PolicyType getPolicyType() {
		return PolicyType.STRICT;   // we don't support different policy
	}

	@Override
	public void reset() throws ComponentNotReadyException {
	}

	@Override
	public Object getPosition() {
		try{
			return reader.getPosition();
		}catch(Exception ex){
			return -1;
		}
	}

	@Override
	public void movePosition(Object position) throws IOException {
		int pos = 0;
		if (position instanceof Integer) {
			pos = ((Integer) position).intValue();
		} else if (position != null) {
			pos = Integer.parseInt(position.toString());
		}
		if (pos > 0) {
			reader.seek(pos);
		}

	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		// TODO Auto-generated method stub
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		fs = null;
	}

	@Override
	public void free() throws ComponentNotReadyException, IOException {
		reader = null;
	}

	@Override
	public boolean nextL3Source() {
		return false;
	}

	@Override
	public DataSourceType getPreferredDataSourceType() {
		return DataSourceType.URI;
	}

	@Override
	public void setKeyValueFields(String keyFieldName, String valueFieldName) {
		this.keyFieldName = keyFieldName;
		this.valueFieldName = valueFieldName;
	}

	@Override
	public void setMetadata(DataRecordMetadata metadata) {
		this.metadata = metadata;
	}
	
	public void setDFS(FileSystem dfs){
		this.fs = dfs;
	}

	@Override
	public void setHadoopConnection(HadoopFileSystemService conn) {
		this.fs = (FileSystem)conn.getDFS();
	}

	@Override
	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}

}
