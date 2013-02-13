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
package org.jetel.hadoop.service.filesystem;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import org.jetel.hadoop.component.IHadoopSequenceFileFormatter;
import org.jetel.hadoop.component.IHadoopSequenceFileParser;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 */
public interface HadoopFileSystemService {
	
	long getUsedSpace() throws IOException;
	
	String getFSMasterURLTemplate();
	
	Object getDFS();
	
	HadoopDataInput open(URI file) throws IOException;
	HadoopDataInput open(URI file, int bufferSize) throws IOException;
	
	HadoopDataOutput create(URI file, boolean overwrite) throws IOException;
	HadoopDataOutput create(URI file, boolean overwrite, int bufferSize) throws IOException;
	HadoopDataOutput create(URI file, boolean overwrite, int bufferSize, short replication, long blockSize) throws IOException;
	
	boolean createNewFile(URI file) throws IOException;
	
	/**
	 * Creates {@link IHadoopSequenceFileFormatter} implementation, typically backed by Hadoop {@code SequenceFile.Writer} class.
	 * Each input record is written as key-value pair in the sequence file.
	 * @param keyFieldName field name of input record carrying the key of key-value pair
	 * @param valueFieldName field name of input record carrying the value of key-value pair
	 * @param overwrite whether to overwrite file or append to it
	 * @param user user name - used for file system operations
	 * @param hadoopProperties additional properties specified in a Hadoop connection 
	 * @return
	 * @throws IOException
	 */
	IHadoopSequenceFileFormatter createFormatter(String keyFieldName, String valueFieldName, boolean overwrite, String user, Properties hadoopProperties) throws IOException;
	
	/**
	 * Creates {@link IHadoopSequenceFileParser} implementation, typically backed by Hadoop {@code SequenceFile.Reader} class.
	 * Each key-value pair in the sequence file is read into output records. 
	 * @param keyFieldName field name of output record where in which to write the key of read key-value pair
	 * @param valueFieldName field name of output record where in which to write the value of read key-value pair
	 * @param metadata metadata of output edge
	 * @param user user name - used for file system operations
	 * @param hadoopProperties additional properties specified in a Hadoop connection
	 * @return
	 * @throws IOException
	 */
	IHadoopSequenceFileParser createParser(String keyFieldName, String valueFieldName, DataRecordMetadata metadata, String user, Properties hadoopProperties) throws IOException;
	
	HadoopDataOutput append(URI file) throws IOException;
	HadoopDataOutput append(URI file, int bufferSize) throws IOException;
	
	boolean delete(URI file, boolean recursive) throws IOException;
	boolean exists(URI file) throws IOException;
	boolean mkdir(URI file) throws IOException;
	boolean rename(URI src, URI dst) throws IOException;
	HadoopFileStatus[] listStatus(URI path) throws IOException;
	HadoopFileStatus[] globStatus(String glob) throws IOException;
	HadoopFileStatus getStatus(URI path) throws IOException;
	HadoopFileStatus getExtendedStatus(URI path) throws IOException;
	
	void setLastModified(URI path, long lastModified) throws IOException;
}
