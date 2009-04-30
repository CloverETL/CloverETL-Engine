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

import org.apache.commons.logging.Log;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @created     March 25, 2009
 * @see         org.jetel.component.RecordGenerate
 */
public class CTLRecordGenerateAdapter implements RecordGenerate {

    public static final String GENERATE_FUNCTION_NAME = "generate";
    
    /**Constructor for the DataRecordTransform object */
    public CTLRecordGenerateAdapter(String srcCode, Log logger) {
    }

	/**
	 *  Performs any necessary initialization before generate() method is called
	 *
	 * @param  targetMetadata  Array of metadata objects describing source data records
	 * @return                        True if successfull, otherwise False
	 */
	public boolean init(Properties parameters, DataRecordMetadata[] targetRecordsMetadata)
			throws ComponentNotReadyException{
		
		return false;
 	}
	
	/**
	 * Generate data for output records.
	 */
	public int generate(DataRecord[] outputRecords) throws TransformException {
		return SKIP;
	}

	public void finished() {
		// TODO Auto-generated method stub
		
	}

	public TransformationGraph getGraph() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getMessage() {
		// TODO Auto-generated method stub
		return null;
	}

	public Object getSemiResult() {
		// TODO Auto-generated method stub
		return null;
	}

	public void reset() {
		// TODO Auto-generated method stub
		
	}

	public void setGraph(TransformationGraph graph) {
		// TODO Auto-generated method stub
		
	}

	public void signal(Object signalObject) {
		// TODO Auto-generated method stub
		
	}
}

