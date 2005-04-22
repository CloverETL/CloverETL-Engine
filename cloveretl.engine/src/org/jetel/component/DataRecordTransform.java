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
package org.jetel.component;

import java.util.Properties;
import org.jetel.graph.TransformationGraph;
import org.jetel.database.DBConnection;
import org.jetel.data.DataRecord;
import org.jetel.data.lookup.LookupTable;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  Base class used for generating any kind of data transformation. Subclass this
 * one to create your own transformations. It implements basic stubs for all of
 * the RecordTransform interface methods.
 *
 * @author      dpavlis
 * @since       November 1, 2003
 * @revision    $Revision$
 * @created     November 1, 2003
 */

public abstract class DataRecordTransform implements RecordTransform {

	protected String transformName;
	/**
	 * Use <code>errorMessage</code> to report details of problems
	 * which occured within transform method.<br>
	 * Ideally, within catch() section assign meaningful message to
	 * errorMessage field.
	 */
	protected String errorMessage;
	
	protected Properties parameters;
	protected DataRecordMetadata[] sourceMetadata;
	protected DataRecordMetadata[] targetMetadata;


	/**
	 *Constructor for the DataRecordTransform object
	 *
	 * @param  transformName  Any name assigned to this transformation.
	 */
	public DataRecordTransform(String transformName) {
		this.transformName = transformName;
	}


	/**Constructor for the DataRecordTransform object */
	public DataRecordTransform() {
		this.transformName = "anonymous";
	}


	/**
	 *  Performs any necessary initialization before transform() method is called
	 *
	 * @param  sourceMetadata  Array of metadata objects describing source data records
	 * @param  targetMetadata  Array of metadata objects describing source data records
	 * @return                        True if successfull, otherwise False
	 */
	public boolean init(Properties parameters, DataRecordMetadata[] sourceRecordsMetadata, DataRecordMetadata[] targetRecordsMetadata) {
		this.parameters=parameters;
		this.sourceMetadata=sourceRecordsMetadata;
		this.targetMetadata=targetRecordsMetadata;
	    return init();
	}

	/**
	 * Method to be overriden by user. It is called by init(Properties,DataRecordMetadata[],DataRecordMetadata[])
	 * method when all standard initialization is performed.<br>
	 * This implementation is just skeleton. User should provide its own.
	 * 
	 * @return	true if user's initialization was performed successfully
	 */
	public boolean init(){
	    return true;
	}
	
	/**
	 *  Transforms source data records into target data records. Derived class should perform
	 *  this functionality.<br> This basic version only copies content of inputRecord into 
	 * outputRecord field by field. See DataRecord.copyFieldsByPosition() method.
	 *
	 * @param  sourceRecords  input data records (an array)
	 * @param  targetRecords  output data records (an array)
	 * @return                True if transformation was successfull, otherwise False
	 * @see		org.jetel.data.DataRecord#copyFieldsByPosition()
	 */
	public abstract boolean transform(DataRecord[] inputRecords, DataRecord[] outputRecords);
	

	/**
	 * This default transformation only copies content of inputRecords into 
	 * outputRecords field by field. See DataRecord.copyFieldsByPosition() method.
	 *
	 * 
	 * @param inputRecords	input data records (an array)
	 * @param outputRecords output data records (an array)
	 * @return true if successful
	 */
	protected boolean defaultTransform(DataRecord[] inputRecords, DataRecord[] outputRecords){
	    for (int i = 0; i < inputRecords.length; i++) {
			outputRecords[i].copyFieldsByPosition(inputRecords[i]);
		}
		return true;
	}

	/**
	 *  Returns description of error if one of the methods failed
	 *
	 * @return    Error message
	 * @since     April 18, 2002
	 */
	public String getMessage() {
		return errorMessage;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.component.RecordTransform#signal()
	 * In this implementation does nothing.
	 */
	public void signal(Object signalObject){
		
	}
	
	
	/* (non-Javadoc)
	 * @see org.jetel.component.RecordTransform#getSemiResult()
	 */
	public Object getSemiResult(){
		return null;
	}
	
	/**
	 * Returns DBConnection object registered with transformation
	 * graph under specified name (ID).
	 * 
	 * @param id ID of DBConnection under which it was registered with graph. It
	 * translates to ID if graph is loaded from XML
	 * @return DBConnection object if found, otherwise NULL
	 */
	public final DBConnection getDBConnection(String id){
	    return TransformationGraph.getReference().getDBConnection(id);
	}
	
	/**
	 * Returns DataRecordMetadata object registered with transformation
	 * graph under specified name (ID).
	 * 
	 * @param id ID of DataRecordMetadata under which it was registered with graph. It
	 * translates to ID if graph is loaded from XML
	 * @return DataRecordMetadata object if found, otherwise NULL
	 */
	public final DataRecordMetadata getDataRecordMetadata(String id){
	    return TransformationGraph.getReference().getDataRecordMetadata(id);
	}
	
	/**
	 * Returns LookupTable object registered with transformation
	 * graph under specified name (ID).
	 * 
	 * @param id ID of LookupTable under which it was registered with graph. It
	 * translates to ID if graph is loaded from XML
	 * @return LookupTable object if found, otherwise NULL
	 */
	public final LookupTable getLookupTable(String id){
	    return TransformationGraph.getReference().getLookupTable(id);
	}

}

