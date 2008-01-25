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
import org.jetel.exception.JetelException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;
import org.jetel.interpreter.data.TLValue;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  
 *
 * @author      dpavlis
 * @since       June 25, 2006
 * @revision    $Revision: $
 * @created     June 25, 2006
 * @see         org.jetel.component.RecordTransform
 */

public class RecordTransformTL implements RecordTransform {

    public static final String TRANSFORM_FUNCTION_NAME="transform";
    public static final String FINISHED_FUNCTION_NAME="finished";
    public static final String INIT_FUNCTION_NAME="init";
    public static final String RESET_FUNCTION_NAME="reset";
    
    protected TransformationGraph graph;
    protected Log logger;

    protected String errorMessage;
	 
	protected WrapperTL wrapper;

    /**Constructor for the DataRecordTransform object */
    public RecordTransformTL(String srcCode, Log logger) {
        this.logger=logger;
        wrapper = new WrapperTL(srcCode, logger);
    }

	/**
	 *  Performs any necessary initialization before transform() method is called
	 *
	 * @param  sourceMetadata  Array of metadata objects describing source data records
	 * @param  targetMetadata  Array of metadata objects describing source data records
	 * @return                        True if successfull, otherwise False
	 */
	public boolean init(Properties parameters, DataRecordMetadata[] sourceRecordsMetadata, DataRecordMetadata[] targetRecordsMetadata)
			throws ComponentNotReadyException{
		wrapper.setMetadata(sourceRecordsMetadata, targetRecordsMetadata);
		wrapper.setParameters(parameters);
		if (graph != null){
	        wrapper.setGraph(graph);
		}
		wrapper.init();
		TLValue result = null;
		try {
			result = wrapper.execute(INIT_FUNCTION_NAME,null);
		} catch (JetelException e) {
			//do nothing: function init is not necessary
		}
		
		wrapper.prepareFunctionExecution(TRANSFORM_FUNCTION_NAME);
		
		return result == null ? true : (result==TLValue.TRUE_VAL);
 	}

	
	public  boolean transform(DataRecord[] inputRecords, DataRecord[] outputRecords)
			throws TransformException{
		TLValue result = wrapper.executePreparedFunction(inputRecords,
				outputRecords,null);
		return result == null ? true : (result==TLValue.TRUE_VAL);
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
	
	
	/* (non-Javadoc)
	 * @see org.jetel.component.RecordTransform#finished()
	 */
	public void finished(){
        // execute finished transformFunction
		try {
			wrapper.execute(FINISHED_FUNCTION_NAME,null);
		} catch (JetelException e) {
			//do nothing: function finished is not necessary
		}
	}
	
    /* (non-Javadoc)
     * @see org.jetel.component.RecordTransform#setGraph(org.jetel.graph.TransformationGraph)
     */
    public void setGraph(TransformationGraph graph) {
        this.graph = graph;
    }

    /* (non-Javadoc)
     * @see org.jetel.component.RecordTransform#getGraph()
     */
    public TransformationGraph getGraph() {
        return graph;
    }

    /*
     * (non-Javadoc)
     * @see org.jetel.component.RecordTransform#reset()
     */
	public void reset() {
        // execute reset transformFunction
		try {
			wrapper.execute(RESET_FUNCTION_NAME,null);
		} catch (JetelException e) {
			//do nothing: function reset is not necessary
		}
	}
}

