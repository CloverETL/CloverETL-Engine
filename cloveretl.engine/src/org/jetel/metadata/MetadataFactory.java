/*
*    jETeL/Clover.ETL - Java based ETL application framework.
*    Copyright (C) 2002-2004  David Pavlis <david_pavlis@hotmail.com>
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
*/

package org.jetel.metadata;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

import org.jetel.database.SQLUtil;
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
	public static DataRecordMetadata fromFile(String fileURL) throws IOException {
		URL url;
		try{
			url = new URL(fileURL); 
		}catch(MalformedURLException e){
			// try to patch the url
			try {
				url=new URL("file:"+fileURL);
			}catch(MalformedURLException ex){
				throw new RuntimeException("Wrong URL of file specified: "+ex.getMessage());
			}
		}
		DataRecordMetadata recordMetadata;
		DataRecordMetadataXMLReaderWriter metadataXMLRW = new DataRecordMetadataXMLReaderWriter();
		try{
		recordMetadata=metadataXMLRW.read(
				new BufferedInputStream(url.openStream()));
			if (recordMetadata==null){
				throw new RuntimeException("Can't parse metadata definitio file: "+fileURL);
			}
		}catch(Exception ex){
			throw new RuntimeException("Can't get metadata file "+fileURL+" - "+ex.getClass().getName()+" : "+ex.getMessage()); 
		}
		return recordMetadata;
	}
	
	/**
	 * Generates DataRecordMetadata based on DBConnection and SQL query.<br>
	 * The sql query is executed against
	 * database and metadata of result set is obtained. This metadata is translated
	 * to corresponding Clover metadata.
	 * 
	 * @param dbConnection database connection to use
	 * @param sqlQuery sql query to use. 
	 * @return
	 * @throws SQLException
	 */
	public static DataRecordMetadata fromJDBC(DataRecordMetadataJDBCStub metadataStub) throws SQLException{
		Statement statement;
		ResultSet resultSet;
		
		statement = metadataStub.getDBConnection().getStatement();
		resultSet = statement.executeQuery(metadataStub.getSQLQuery());
		return SQLUtil.dbMetadata2jetel(resultSet.getMetaData());
	}

	/**
	 * Generates DataRecordMetadata based on XML definition
	 * 
	 * @param node
	 * @return
	 */
	public static DataRecordMetadata fromXML(org.w3c.dom.Node node)throws DOMException{
		DataRecordMetadataXMLReaderWriter parser=new DataRecordMetadataXMLReaderWriter();
		return parser.parseRecordMetadata(node);
	}
	
}
