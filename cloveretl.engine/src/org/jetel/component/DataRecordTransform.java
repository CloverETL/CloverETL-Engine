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

import org.jetel.data.DataRecord;
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

public class DataRecordTransform implements RecordTransform {

	private String transformName;
	/**
	 * Use <code>errorMessage</code> to report details of problems
	 * which occured within transform method.<br>
	 * Ideally, within catch() section assign meaningful message to
	 * errorMessage field.
	 */
	protected String errorMessage;


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
	 * @param  sourceRecordsMetadata  Array of metadata objects describing source data records
	 * @param  targetRecordsMetadata  Array of metadata objects describing source data records
	 * @return                        True if successfull, otherwise False
	 */
	public boolean init(DataRecordMetadata[] sourceRecordsMetadata, DataRecordMetadata[] targetRecordsMetadata) {
		return true;
	}

	
	/**
	 *  Initializes reformat class/function. This method is called only once at the
	 * beginning of transforming process. Any object allocation/initialization should
	 * happen here.
	 *
	 *@param  sourceMetadata  Metadata describing source data record
	 *@param  targetMetadata  Metadata describing target data record
	 *@return                 True if OK, otherwise False
	 *@since                  April 18, 2002
	 */
	public boolean init(DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata){
		DataRecordMetadata in[]= {sourceMetadata};
		DataRecordMetadata out[]= {targetMetadata};
		return init(in,out);
	}

	
	
	/**
	 *  Transforms source data records into target data records. Derived class should perform
	 *  this functionality.<br> This basic version only copies content of inputRecord into 
	 * outputRecord field by field. See DataRecord.copyFieldsByPosition() method.
	 *
	 * @param  sourceRecords  Description of the Parameter
	 * @param  targetRecords  Description of the Parameter
	 * @return                True if transformation was successfull, otherwise False
	 * @see		org.jetel.data.DataRecord#copyFieldsByPosition()
	 */
	public boolean transform(DataRecord[] inputRecords, DataRecord[] outputRecords){
		for (int i = 0; i < inputRecords.length; i++) {
			outputRecords[i].copyFieldsByPosition(inputRecords[i]);
		}
		return true;
	
	}


	public boolean transform(DataRecord source, DataRecord target){
		DataRecord in[]={source};
		DataRecord out[]={target};
		return transform(in,out);
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
	 */
	public void signal(){
		
	}
	
	
	/* (non-Javadoc)
	 * @see org.jetel.component.RecordTransform#getSemiResult()
	 */
	public Object getSemiResult(){
		return null;
	}
	

}

