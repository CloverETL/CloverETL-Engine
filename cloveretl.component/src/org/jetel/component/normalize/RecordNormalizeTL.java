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

import java.io.ByteArrayInputStream;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.jetel.data.DataRecord;
import org.jetel.data.primitive.CloverInteger;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.interpreter.ParseException;
import org.jetel.interpreter.TransformLangExecutor;
import org.jetel.interpreter.TransformLangParser;
import org.jetel.interpreter.node.CLVFFunctionDeclaration;
import org.jetel.interpreter.node.CLVFStart;
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

    private String srcCode;
    private Log logger;

	protected Properties parameters;
    private TransformLangExecutor executor;
    private CLVFFunctionDeclaration lengthFunction, transformFunction, finishedFunction, initFunction;

    private String errorMessage;

    /**Constructor for the DataRecordTransform object */
    public RecordNormalizeTL(Log logger,String srcCode) {
        this.srcCode=srcCode;
        this.logger=logger;
    }

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordNormalize#init(java.util.Properties, org.jetel.metadata.DataRecordMetadata, org.jetel.metadata.DataRecordMetadata)
	 */
	public boolean init(Properties parameters,
			DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
			throws ComponentNotReadyException {
        CLVFStart parseTree=null;
        TransformLangParser parser = new TransformLangParser(new DataRecordMetadata[]{sourceMetadata},
        		new DataRecordMetadata[]{targetMetadata},new ByteArrayInputStream(srcCode.getBytes()));
        
        try {
            parseTree = parser.Start();
            parseTree.init();
        }catch(ParseException ex){
            logger.error(ex);
            errorMessage=ex.getMessage();
            return false;
        }catch(Exception ex){
            logger.error(ex);
            errorMessage=ex.getMessage();
            return false;
        }
        
        // log & report any parse exceptions
        for(Iterator iter=parser.getParseExceptions().iterator();iter.hasNext();){
            logger.error(iter.next());
        }
        
        if (parser.getParseExceptions().size()>0){
            errorMessage=((Exception)parser.getParseExceptions().get(0)).getMessage();
            return false;
        }
        
        executor=new TransformLangExecutor(parameters);
        lengthFunction=(CLVFFunctionDeclaration)parser.getFunctions().get(LENGTH_FUNCTION_NAME);
        transformFunction=(CLVFFunctionDeclaration)parser.getFunctions().get(TRANSFORM_FUNCTION_NAME);
        finishedFunction=(CLVFFunctionDeclaration)parser.getFunctions().get(FINISHED_FUNCTION_NAME);
        initFunction=(CLVFFunctionDeclaration)parser.getFunctions().get(INIT_FUNCTION_NAME);
        if (transformFunction==null){
            errorMessage="Transformation transformFunction not declared/defined";
            logger.error(errorMessage);
            return false;
        }
        if (lengthFunction==null){
            errorMessage="Transformation lengthFunction not declared/defined";
            logger.error(errorMessage);
            return false;
        }
       
        // execute global declarations, etc
        try{
            executor.visit(parseTree,null);
        }catch (Exception ex){
            logger.error(ex);
            errorMessage=ex.getMessage();
            return false;
        }
        
        //execute init transformFunction
        if (initFunction!=null){
            executor.executeFunction(initFunction,null); 
        }
        
        this.parameters=parameters;
        
        return true;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordNormalize#count(org.jetel.data.DataRecord)
	 */
	public int count(DataRecord source) {
        executor.setInputRecords(new DataRecord[]{source});
		executor.setOutputRecords(new DataRecord[]{});

		// execute lengthFunction
		executor.executeFunction(lengthFunction, null);

		CloverInteger result = (CloverInteger) executor.getResult();
		return result.intValue();
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordNormalize#transform(org.jetel.data.DataRecord, org.jetel.data.DataRecord, int)
	 */
	public boolean transform(DataRecord source, DataRecord target, int idx)
			throws TransformException {
        executor.setInputRecords(new DataRecord[]{source});
		executor.setOutputRecords(new DataRecord[]{target});

		// execute transformation transformFunction
		executor.executeFunction(transformFunction, new Object[]{new CloverInteger(idx)});

		Boolean result = (Boolean) executor.getResult();
		return result != null ? result.booleanValue() : true;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordNormalize#finished()
	 */
	public void finished() {
		if (finishedFunction != null) {
			executor.executeFunction(finishedFunction, null);
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordNormalize#getMessage()
	 */
	public String getMessage() {
		return errorMessage;
	}

}
