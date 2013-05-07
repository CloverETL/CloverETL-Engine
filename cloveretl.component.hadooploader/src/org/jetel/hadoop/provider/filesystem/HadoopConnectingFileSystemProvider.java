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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URI;
import java.util.Properties;

import javax.net.SocketFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.net.NetUtils;
import org.jetel.hadoop.component.IHadoopSequenceFileFormatter;
import org.jetel.hadoop.component.IHadoopSequenceFileParser;
import org.jetel.hadoop.provider.HadoopConfigurationUtils;
import org.jetel.hadoop.service.filesystem.HadoopConnectingFileSystemService;
import org.jetel.hadoop.service.filesystem.HadoopDataInput;
import org.jetel.hadoop.service.filesystem.HadoopDataOutput;
import org.jetel.hadoop.service.filesystem.HadoopFileStatus;
import org.jetel.hadoop.service.filesystem.HadoopFileSystemConnectionData;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author dpavlis (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Mar 14, 2012
 * 
 * @see FileSystem
 */

public class HadoopConnectingFileSystemProvider implements HadoopConnectingFileSystemService {

	public static final String NAMENODE_URL_TEMPLATE = "hdfs://%s:%s";
	public static final String NAMENODE_URL_KEY = "fs.default.name";

	private static final int CONNECTION_TEST_TIMEOUT = 10000; // ms
	
	private FileSystem dfs;

	@Override
	public void connect(HadoopFileSystemConnectionData connData, Properties additionalSettings) throws IOException {
		if (isConnected()) {
			throw new IllegalStateException(DUPLICATED_CONNECT_CALL_MESSAGE);
		}
		if (connData == null) {
			throw new NullPointerException("connData");
		}
		
		Configuration config = HadoopConfigurationUtils.property2Configuration(additionalSettings);
		config.set(NAMENODE_URL_KEY, String.format(NAMENODE_URL_TEMPLATE, connData.getHost(), connData.getPort()));
		
		// Just try to connect to host first (real connect to HDFS makes 45 socket-timeout attempts, each timeout's 20s long)
		connectionTest(FileSystem.getDefaultUri(config), config);
		
		try {
			dfs = FileSystem.get(FileSystem.getDefaultUri(config), config, connData.getUser());
		} catch (InterruptedException ex) {
			throw new RuntimeException("Hadoop client API internal exception occured.", ex);
		}
	}
	
	// code taken from org.apache.hadoop.ipc.Client.Connection.setupConnection()
	private void connectionTest(URI host, Configuration hadoopConfiguration) throws IOException {
		SocketFactory socketFactory = NetUtils.getSocketFactory(hadoopConfiguration, ClientProtocol.class);
		Socket socket = socketFactory.createSocket();
        socket.setTcpNoDelay(false);
        SocketAddress address = new InetSocketAddress(host.getHost(), host.getPort());
        try {
        	NetUtils.connect(socket, address, CONNECTION_TEST_TIMEOUT);
		} finally {
			try {
				socket.close();
			} catch (IOException e) {}
		}
	}

	@Override
	public String getFSMasterURLTemplate() {
		return NAMENODE_URL_TEMPLATE;
	}

	@Override
	public boolean isConnected() {
		return dfs != null;
	}

	@Override
	public String validateConnection() throws IOException {
		if (!isConnected()) {
			throw new IllegalStateException(NOT_CONNECTED_MESSAGE);
		}
		dfs.getStatus().getUsed();
		return null;
	}

	@Override
	public void close() throws IOException {
		try {
			if (dfs != null) {
				dfs.close();
			}
		} finally {
			dfs = null;
		}
	}

	@Override
	public long getUsedSpace() throws IOException {
		checkConnected();
		return dfs.getUsed();
	}

	@Override
	public HadoopDataInput open(URI file) throws IOException {
		checkConnected();
		Path path=new Path(file);
		long length=dfs.getFileStatus(path).getLen();
		return new HadoopDataInputStream(dfs.open(path),length);
	}

	@Override
	public HadoopDataInput open(URI file, int bufferSize) throws IOException {
		checkConnected();
		Path path=new Path(file);
		long length=dfs.getFileStatus(path).getLen();
		return new HadoopDataInputStream(dfs.open(path, bufferSize),length);
	}

	@Override
	public HadoopDataOutput create(URI file, boolean overwrite) throws IOException {
		checkConnected();
		return new HadoopDataOutputStream(dfs.create(new Path(file), overwrite));
	}

	@Override
	public HadoopDataOutput create(URI file, boolean overwrite, int bufferSize) throws IOException {
		checkConnected();
		return new HadoopDataOutputStream(dfs.create(new Path(file), overwrite, bufferSize));
	}

	@Override
	public HadoopDataOutput create(URI file, boolean overwrite, int bufferSize, short replication, long blockSize)
			throws IOException {
		checkConnected();
		return new HadoopDataOutputStream(dfs.create(new Path(file), overwrite, bufferSize, replication, blockSize));
	}

	@Override
	public IHadoopSequenceFileFormatter createFormatter(String keyFieldName, String valueFieldName, boolean overwrite, String user, Properties hadoopProperties)
			throws IOException {
		return new HadoopSequenceFileFormatter(keyFieldName, valueFieldName, user, HadoopConfigurationUtils.property2Configuration(hadoopProperties));
	}

	@Override
	public HadoopDataOutput append(URI file) throws IOException {
		checkConnected();
		return new HadoopDataOutputStream(dfs.append(new Path(file)));
	}

	@Override
	public HadoopDataOutput append(URI f, int bufferSize) throws IOException {
		checkConnected();
		return new HadoopDataOutputStream(dfs.append(new Path(f), bufferSize));
	}

	@Override
	public boolean delete(URI file, boolean recursive) throws IOException {
		checkConnected();
		return dfs.delete(new Path(file), recursive);
	}

	@Override
	public boolean exists(URI file) throws IOException {
		checkConnected();
		return dfs.exists(new Path(file));
	}

	@Override
	public boolean mkdir(URI file) throws IOException {
		checkConnected();
		return dfs.mkdirs(new Path(file));
	}

	@Override
	public boolean rename(URI src, URI dst) throws IOException {
		checkConnected();
		return dfs.rename(new Path(src), new Path(dst));
	}

	@Override
	public HadoopFileStatus[] listStatus(URI path) throws IOException {
		checkConnected();
		FileStatus[] status = dfs.listStatus(new Path(path));
		if (status == null) {
			throw new IOException("Can't get HDFS file(s) status for: " + path.toString());
		}
		HadoopFileStatus[] hStatus = new HadoopFileStatus[status.length];
		for (int i = 0; i < status.length; i++) {
			hStatus[i] = new HadoopFileStatus(status[i].getPath().toUri(), status[i].getLen(), status[i].isDir(),
					status[i].getModificationTime());
		}
		return hStatus;
	}

	@Override
	public IHadoopSequenceFileParser createParser(String keyFieldName, String valueFieldName,
			DataRecordMetadata metadata, String user, Properties hadoopProperties) throws IOException {
		return new HadoopSequenceFileParser(metadata, keyFieldName, valueFieldName, user, HadoopConfigurationUtils.property2Configuration(hadoopProperties));
	}

	@Override
	public HadoopFileStatus getStatus(URI path) throws IOException {
		checkConnected();
		FileStatus status = dfs.getFileStatus(new Path(path));
		return new HadoopFileStatus(status.getPath().toUri(), status.getLen(), status.isDir(),
				status.getModificationTime());
	}

	@Override
	public HadoopFileStatus getExtendedStatus(URI path) throws IOException {
		checkConnected();
		FileStatus status = dfs.getFileStatus(new Path(path));
		return new HadoopFileStatus(status.getPath().toUri(), status.getLen(), status.isDir(),
				status.getModificationTime(), status.getBlockSize(), status.getGroup(), status.getOwner(),
				status.getReplication());
	}

	@Override
	public Object getDFS() {
		checkConnected();
		return dfs;
	}


	@Override
	public void setLastModified(URI path, long lastModified) throws IOException {
		checkConnected();
		dfs.setTimes(new Path(path), lastModified, -1);
	}

	@Override
	public HadoopFileStatus[] globStatus(String glob) throws IOException {
		checkConnected();
		
		glob = glob.replace("\\", "%25");
		Path path = new Path(glob);
		
		FileStatus[] status = dfs.globStatus(path);
		if (status == null) {
			return null;
		}
		HadoopFileStatus[] hStatus = new HadoopFileStatus[status.length];
		for (int i = 0; i < status.length; i++) {
			hStatus[i] = new HadoopFileStatus(status[i].getPath().toUri(), status[i].getLen(), status[i].isDir(),
					status[i].getModificationTime());
		}
		return hStatus;
	}

	@Override
	public boolean createNewFile(URI path) throws IOException {
		checkConnected();
		return dfs.createNewFile(new Path(path));
	}

	private void checkConnected() {
		if (!isConnected()) {
			throw new IllegalStateException(NOT_CONNECTED_MESSAGE);
		}
	}
}
