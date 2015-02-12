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

import java.util.Properties;

import org.jetel.database.IConnection;
import org.jetel.exception.JetelRuntimeException;


/**
 * @author dpavlis
 * @since  20.10.2004
 *
 * Stub class to keep certain parameters for metadata generation
 * based on IConnection. Used during loading graph definition
 * from XML.
 */
public class DataRecordMetadataStub {

	private String metadataId;
	
	private IConnection connection;
    
	private Properties parameters;

	/**
	 * Cached instance of metadata created by {@link #createMetadata()} method.
	 */
	private DataRecordMetadata metadataCache;
	/**
	 * Exception possibly thrown by {@link #createMetadata()} method.
	 * If the method fails, the exception is thrown from each other invocation of the method.  
	 */
	private RuntimeException dbFailure;
	
	public DataRecordMetadataStub(IConnection connection, Properties parameters){
		this.connection = connection;
		this.parameters = parameters;
	}
	
	public IConnection getConnection() {
		return connection;
	}
	
	public Properties getParameters() {
		return parameters;
	}
    
    public DataRecordMetadata createMetadata() {
    	if (dbFailure != null) {
    		//last invocation failed, throw the identical exception
    		throw dbFailure;
    	} else if (metadataCache == null) {
    		//no metadata cached, let's try to load metadata from database
	    	try {
		        connection.init();
		        metadataCache = connection.createMetadata(parameters);
		        metadataCache.setId(metadataId);
	    	} catch (Exception e) {
	    		dbFailure = new JetelRuntimeException("Creating metadata (id='" + metadataId + "') from DB connection failed.", e);
	    		throw dbFailure;
	    	}
    	}
    	
    	return metadataCache;
    }

	/**
	 * Sets metadata identifier which will be used for metadata created by this stub.
	 * @param metadataID
	 */
	public void setId(String metadataId) {
		this.metadataId = metadataId;
	}
}
