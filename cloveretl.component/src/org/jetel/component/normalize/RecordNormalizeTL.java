/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
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
package org.jetel.component.normalize;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.jetel.component.WrapperTL;
import org.jetel.data.DataRecord;
import org.jetel.data.primitive.CloverInteger;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;
import org.jetel.interpreter.data.TLBooleanValue;
import org.jetel.interpreter.data.TLNumericValue;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Implements normalization based on TransformLang source specified by user. 
 * User defines following functions (asterisk denotes the mandatory ones):<ul>
 * <li>* function count()</li>
 * <li>* function transform(idx)</li>
 * <li>function init()</li> 
 * <li>function finished()</li>
 * </ul>
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 11/21/06  
 * @see org.jetel.component.Normalizer
 */
public class RecordNormalizeTL implements RecordNormalize {

	private static final String LENGTH_FUNCTION_NAME="count";
	private static final String TRANSFORM_FUNCTION_NAME="transform";
    private static final String FINISHED_FUNCTION_NAME="finished";
    private static final String INIT_FUNCTION_NAME="init";
    private static final String CLEAN_FUNCTION_NAME="clean";
    
    private int lenghtFunctionIdentifier;
    private int transformFunctionIdentifier;
    private int cleanFunctionIdentifier;

    private String errorMessage;
    private WrapperTL wrapper;
    private TLValue[] counterTL;
    private DataRecord[] sourceRec;
    private DataRecord[] targetRec;
    
    /**Constructor for the DataRecordTransform object */
    public RecordNormalizeTL(Log logger,String srcCode,TransformationGraph graph) {
         wrapper = new WrapperTL(srcCode,logger);
         wrapper.setGraph(graph);
         counterTL = new TLValue[]{new TLNumericValue<CloverInteger>(TLValueType.INTEGER,new CloverInteger(0))};
         sourceRec=new DataRecord[1];
         targetRec=new DataRecord[1];

    }

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordNormalize#init(java.util.Properties, org.jetel.metadata.DataRecordMetadata, org.jetel.metadata.DataRecordMetadata)
	 */
	public boolean init(Properties parameters,
			DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
			throws ComponentNotReadyException {
		wrapper.setMetadata(new DataRecordMetadata[]{sourceMetadata}, 
				new DataRecordMetadata[]{targetMetadata});
		wrapper.setParameters(parameters);
		wrapper.init();
		TLValue result = null;
		try {
			result = wrapper.execute(INIT_FUNCTION_NAME,null);
		} catch (JetelException e) {
			//do nothing: function init is not necessary
		}
		
		lenghtFunctionIdentifier = wrapper.prepareFunctionExecution(LENGTH_FUNCTION_NAME);
		transformFunctionIdentifier = wrapper.prepareFunctionExecution(TRANSFORM_FUNCTION_NAME);
		try{
			cleanFunctionIdentifier = wrapper.prepareFunctionExecution(CLEAN_FUNCTION_NAME);
		}catch(Exception ex){
			//do nothing
			cleanFunctionIdentifier=-1;
		}
		
		return result == null ? true : result==TLBooleanValue.TRUE;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordNormalize#count(org.jetel.data.DataRecord)
	 */
	public int count(DataRecord source) {
		TLValue value;
		value=wrapper.executePreparedFunction(lenghtFunctionIdentifier,source,null);
		if (value.type.isNumeric()){
			return ((TLNumericValue)value).getInt();
		}else{
			throw new RuntimeException("Normalizer - count() functions does not return integer value !");
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordNormalize#transform(org.jetel.data.DataRecord, org.jetel.data.DataRecord, int)
	 */
	public int transform(DataRecord source, DataRecord target, int idx) throws TransformException {
		counterTL[0].getNumeric().setValue(idx);
		sourceRec[0]=source;
		targetRec[0]=target;

		TLValue result = wrapper.executePreparedFunction(transformFunctionIdentifier, 
				sourceRec, targetRec, counterTL);

		if (result == null || result == TLBooleanValue.TRUE) {
			return 0;
		}

		if (result.getType().isNumeric()) {
			return result.getNumeric().getInt();
		}

		errorMessage = "Unexpected return result: " + result.toString() + " (" + result.getType().getName() + ")";

		return -1;
	}

	public void clean(){
		if (cleanFunctionIdentifier!=-1){
			wrapper.executePreparedFunction(cleanFunctionIdentifier);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.component.RecordNormalize#finished()
	 */
	public void finished() {
		try {
			wrapper.execute(FINISHED_FUNCTION_NAME,null);
		} catch (JetelException e) {
			//do nothing: function finished is not necessary
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordNormalize#getMessage()
	 */
	public String getMessage() {
		return errorMessage;
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.component.normalize.RecordNormalize#reset()
	 */
	public void reset() {
		wrapper.reset();
		errorMessage = null;
	}

}
