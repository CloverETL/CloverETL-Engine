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
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jetel.hadoop.component.IHadoopSequenceFileFormatter;
import org.jetel.hadoop.component.IHadoopSequenceFileParser;
import org.jetel.hadoop.connection.HadoopFileStatus;
import org.jetel.hadoop.connection.IHadoopConnection;
import org.jetel.hadoop.connection.IHadoopInputStream;
import org.jetel.hadoop.connection.IHadoopOutputStream;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author dpavlis (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Mar 14, 2012
 * 
 * @see org.apache.hadoop.fs.FileSystem
 */

public class HadoopConnectionInstance implements IHadoopConnection {

	// private static final Log logger =
	// LogFactory.getLog(HadoopConnectionInstance.class);
	private FileSystem dfs;

	public HadoopConnectionInstance() {}

	@Override
	public boolean connect(URI host, Properties config) throws IOException {
		return connect(host, config, null);
	}

	@Override
	public boolean connect(URI host, Properties config, String user) throws IOException {
		// logger.debug(host);
		// logger.debug(user);
		ClassLoader formerCCL = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
		try {
			if (user != null) {
				dfs = FileSystem.get(host, HadoopConfigurationUtil.property2Configuration(config), user);
			} else {
				dfs = FileSystem.get(host, HadoopConfigurationUtil.property2Configuration(config));
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		} catch (Throwable e) {
			throw new IOException("Class loading error !", e);
		} finally {
			Thread.currentThread().setContextClassLoader(formerCCL);
		}

		return true;
	}

	@Override
	public long getUsedSpace() throws IOException {
		if (dfs != null) {
			return dfs.getUsed();
		} else {
			throw new IOException("Not connected to HDFS.");
		}
	}

	@Override
	public void close() throws IOException {
		if (dfs != null) {
			dfs.close();
		}
		dfs = null;
	}

	@Override
	public IHadoopInputStream open(URI file) throws IOException {
		if (dfs != null) {
			return new HadoopInputStream(dfs.open(new Path(file)));
		} else {
			throw new IOException("Not connected to HDFS.");
		}

	}

	@Override
	public IHadoopInputStream open(URI file, int bufferSize) throws IOException {
		if (dfs != null) {
			return new HadoopInputStream(dfs.open(new Path(file), bufferSize));
		} else {
			throw new IOException("Not connected to HDFS.");
		}
	}

	@Override
	public IHadoopOutputStream create(URI file, boolean overwrite) throws IOException {
		if (dfs != null) {
			return new HadoopOutputStream(dfs.create(new Path(file), overwrite));
		} else {
			throw new IOException("Not connected to HDFS.");
		}
	}

	@Override
	public IHadoopOutputStream create(URI file, boolean overwrite, int bufferSize) throws IOException {
		if (dfs != null) {
			return new HadoopOutputStream(dfs.create(new Path(file), overwrite, bufferSize));
		} else {
			throw new IOException("Not connected to HDFS.");
		}
	}

	@Override
	public IHadoopOutputStream create(URI file, boolean overwrite, int bufferSize, short replication, long blockSize)
			throws IOException {
		if (dfs != null) {
			return new HadoopOutputStream(dfs.create(new Path(file), overwrite, bufferSize, replication, blockSize));
		} else {
			throw new IOException("Not connected to HDFS.");
		}
	}

	@Override
	public IHadoopSequenceFileFormatter createFormatter(String keyFieldName, String valueFieldName, boolean overwrite)
			throws IOException {
		ClassLoader formerCCL = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

		IHadoopSequenceFileFormatter formatter = new HadoopSequenceFileFormatter(this.dfs, keyFieldName, valueFieldName);

		Thread.currentThread().setContextClassLoader(formerCCL);

		return formatter;
	}

	@Override
	public IHadoopOutputStream append(URI file) throws IOException {
		if (dfs != null) {
			return new HadoopOutputStream(dfs.append(new Path(file)));
		} else {
			throw new IOException("Not connected to HDFS.");
		}
	}

	@Override
	public IHadoopOutputStream append(URI f, int bufferSize) throws IOException {
		if (dfs != null) {
			return new HadoopOutputStream(dfs.append(new Path(f), bufferSize));
		} else {
			throw new IOException("Not connected to HDFS.");
		}
	}

	@Override
	public boolean delete(URI file, boolean recursive) throws IOException {
		if (dfs != null) {
			return dfs.delete(new Path(file), recursive);
		} else {
			throw new IOException("Not connected to HDFS.");
		}
	}

	@Override
	public boolean exists(URI file) throws IOException {
		if (dfs != null) {
			return dfs.exists(new Path(file));
		} else {
			throw new IOException("Not connected to HDFS.");
		}
	}

	@Override
	public boolean mkdir(URI file) throws IOException {
		if (dfs != null) {
			return dfs.mkdirs(new Path(file));
		} else {
			throw new IOException("Not connected to HDFS.");
		}
	}

	@Override
	public boolean rename(URI src, URI dst) throws IOException {
		if (dfs != null) {
			return dfs.rename(new Path(src), new Path(dst));
		} else {
			throw new IOException("Not connected to HDFS.");
		}
	}

	@Override
	public HadoopFileStatus[] listStatus(URI path) throws IOException {
		if (dfs != null) {
			FileStatus[] status = dfs.listStatus(new Path(path));
			if (status == null)
				throw new IOException("Can't get HDFS file(s) status for: " + path.toString());
			HadoopFileStatus[] hStatus = new HadoopFileStatus[status.length];
			for (int i = 0; i < status.length; i++) {
				hStatus[i] = new HadoopFileStatus(status[i].getPath().toUri(), status[i].getLen(), status[i].isDir(),
						status[i].getModificationTime());
			}
			return hStatus;

		} else {
			throw new IOException("Not connected to HDFS.");
		}
	}

	@Override
	public boolean connect() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public IHadoopSequenceFileParser createParser(String keyFieldName, String valueFieldName,
			DataRecordMetadata metadata) throws IOException {
		return new HadoopSequenceFileParser(dfs, metadata, keyFieldName, valueFieldName);
	}

	@Override
	public HadoopFileStatus getStatus(URI path) throws IOException {
		FileStatus status = dfs.getFileStatus(new Path(path));
		return new HadoopFileStatus(status.getPath().toUri(), status.getLen(), status.isDir(),
				status.getModificationTime());
	}

	@Override
	public HadoopFileStatus getExtendedStatus(URI path) throws IOException {
		FileStatus status = dfs.getFileStatus(new Path(path));
		return new HadoopFileStatus(status.getPath().toUri(), status.getLen(), status.isDir(),
				status.getModificationTime(), status.getBlockSize(), status.getGroup(), status.getOwner(),
				status.getReplication());
	}

	@Override
	public Object getDFS() {
		return dfs;
	}
}
