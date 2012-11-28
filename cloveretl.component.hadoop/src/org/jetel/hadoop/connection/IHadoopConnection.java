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
package org.jetel.hadoop.connection;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import org.jetel.hadoop.component.IHadoopSequenceFileFormatter;
import org.jetel.hadoop.component.IHadoopSequenceFileParser;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *         
 */
public interface IHadoopConnection {

	public boolean connect(URI host, Properties config) throws IOException;
	public boolean connect(URI host, Properties config, String user) throws IOException;
	public boolean connect() throws IOException;
	
	public long getUsedSpace() throws IOException;
	
	public void close() throws IOException;
	
	public Object getDFS();
	
	public IHadoopInputStream open(URI file) throws IOException;
	public IHadoopInputStream open(URI file, int bufferSize) throws IOException;
	
	public IHadoopOutputStream create(URI file, boolean overwrite) throws IOException;
	public IHadoopOutputStream create(URI file,boolean overwrite, int bufferSize) throws IOException;
	public IHadoopOutputStream create(URI file, boolean overwrite, int bufferSize, short replication, long blockSize) throws IOException;
	
	public IHadoopSequenceFileFormatter createFormatter(String keyFieldName, String valueFieldName,boolean overwrite) throws IOException;
	
	public IHadoopSequenceFileParser createParser(String keyFieldName, String valueFieldName, DataRecordMetadata metadata) throws IOException;
	
	public IHadoopOutputStream append(URI file) throws IOException;
	public IHadoopOutputStream append(URI f, int bufferSize) throws IOException;
	
	public boolean delete(URI file, boolean recursive) throws IOException;
	public boolean exists(URI file) throws IOException;
	public boolean mkdir(URI file) throws IOException;
	public boolean rename(URI src, URI dst) throws IOException;
	public HadoopFileStatus[] listStatus(URI path) throws IOException;
	public HadoopFileStatus getStatus(URI path) throws IOException;
	public HadoopFileStatus getExtendedStatus(URI path) throws IOException;
	
	public void setLastModified(URI path, long lastModified) throws IOException;
}
