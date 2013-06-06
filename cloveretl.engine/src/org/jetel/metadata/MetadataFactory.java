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
package org.jetel.metadata;

import java.io.IOException;
import java.nio.channels.Channels;
import java.sql.SQLException;

import org.jetel.graph.TransformationGraph;
import org.jetel.util.file.FileUtils;
import org.w3c.dom.DOMException;


/**
 * @author dpavlis
 * @since  19.10.2004
 *
 * Helper class for instantiating metadata from various
 * sources. Not really a factory - merely set of metadata creation
 * methods.
 */
public class MetadataFactory {
	
	/**
	 * Parses DataRecordMetadata from file. File can be stored locally or
	 * on HTTP server. 
	 * 
	 * @param fileURL URL of the file. (e.g. file:/home/test/metadata.frm , http://server/metadata/metadata.frm )
	 * @return metadata describing data record
	 * @throws IOException
	 * @see	org.jetel.metadata.DataRecordMetadata
	 */
	public static DataRecordMetadata fromFile(TransformationGraph graph, String fileURL) throws IOException {
		DataRecordMetadata recordMetadata;
		DataRecordMetadataXMLReaderWriter metadataXMLRW = new DataRecordMetadataXMLReaderWriter(graph);
		try{
		recordMetadata=metadataXMLRW.read(
				Channels.newInputStream(FileUtils.getReadableChannel(graph.getRuntimeContext().getContextURL(), fileURL)));
			if (recordMetadata==null){
				throw new RuntimeException("Can't parse metadata definition file: " + fileURL);
			}
	    }catch(IOException ex){
	        throw new IOException("Can't read metadata definition file", ex);
		}catch(Exception ex){
			throw new RuntimeException("Can't get metadata file " + fileURL, ex); 
		}
		return recordMetadata;
	}
	
	/**
	 * Generates DataRecordMetadata based on IConnection and its parameters.<br>
	 * The sql query is executed against
	 * database and metadata of result set is obtained. This metadata is translated
	 * to corresponding Clover metadata.
	 * 
	 * @param dbConnection database connection to use
	 * @param sqlQuery sql query to use. 
	 * @return
	 * @throws Exception 
	 * @throws SQLException
	 */
	public static DataRecordMetadata fromStub(DataRecordMetadataStub metadataStub) throws Exception {
        return metadataStub.createMetadata();
	}

	/**
	 * Generates DataRecordMetadata based on XML definition
	 * 
	 * @param node
	 * @return
	 */
	public static DataRecordMetadata fromXML(TransformationGraph graph, org.w3c.dom.Node node)throws DOMException{
		DataRecordMetadataXMLReaderWriter parser=new DataRecordMetadataXMLReaderWriter(graph);
		return parser.parseRecordMetadata(node);
	}
	
}
