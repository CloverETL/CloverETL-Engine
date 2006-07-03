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

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.jetel.graph.TransformationGraph;
import org.jetel.database.DBConnection;
import org.jetel.data.DataRecord;
import org.jetel.data.lookup.LookupTable;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.interpreter.ParseException;
import org.jetel.interpreter.TransformLangExecutor;
import org.jetel.interpreter.TransformLangParser;
import org.jetel.interpreter.node.CLVFStart;
import org.jetel.interpreter.node.CLVFFunctionDeclaration;

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

    public static final String TL_TRANSFORM_CODE_ID="//#TL";
    
    public static final String TRANSFORM_FUNCTION_NAME="transform";
    public static final String FINISHED_FUNCTION_NAME="finished";
    public static final String INIT_FUNCTION_NAME="init";
    
    protected TransformationGraph graph;
    protected TransformLangExecutor executor;
    protected CLVFFunctionDeclaration transformFunction, finishedFunction, initFunction;
    protected String srcCode;
    protected Log logger;

    protected String errorMessage;
	
	protected Properties parameters;
	protected DataRecordMetadata[] sourceMetadata;
	protected DataRecordMetadata[] targetMetadata;

    /**Constructor for the DataRecordTransform object */
    public RecordTransformTL(Log logger,String srcCode) {
        this.srcCode=srcCode;
        this.logger=logger;
    }

	/**
	 *  Performs any necessary initialization before transform() method is called
	 *
	 * @param  sourceMetadata  Array of metadata objects describing source data records
	 * @param  targetMetadata  Array of metadata objects describing source data records
	 * @return                        True if successfull, otherwise False
	 */
	public boolean init(Properties parameters, DataRecordMetadata[] sourceRecordsMetadata, DataRecordMetadata[] targetRecordsMetadata) {

        CLVFStart parseTree=null;
        TransformLangParser parser = new TransformLangParser(sourceRecordsMetadata,
                targetRecordsMetadata,new ByteArrayInputStream(srcCode.getBytes()));
        
        try {
            parseTree = parser.Start();
            parseTree.init();
        }catch(ParseException ex){
            ex.printStackTrace();
            logger.error(ex);
            errorMessage=ex.getMessage();
            return false;
        }catch(Exception ex){
            ex.printStackTrace();
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
        transformFunction=(CLVFFunctionDeclaration)parser.getFunctions().get(TRANSFORM_FUNCTION_NAME);
        finishedFunction=(CLVFFunctionDeclaration)parser.getFunctions().get(FINISHED_FUNCTION_NAME);
        initFunction=(CLVFFunctionDeclaration)parser.getFunctions().get(INIT_FUNCTION_NAME);
        if (transformFunction==null){
            errorMessage="Transformation transformFunction not declared/defined";
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
		this.sourceMetadata=sourceRecordsMetadata;
		this.targetMetadata=targetRecordsMetadata;
        
        return true;
	}

	
	public  boolean transform(DataRecord[] inputRecords, DataRecord[] outputRecords){
        executor.setInputRecords(inputRecords);
        executor.setOutputRecords(outputRecords);
        
        // execute transformation transformFunction
        executor.executeFunction(transformFunction,null);
        
        Boolean result=(Boolean)executor.getResult();
        if (result!=null){
            return result.booleanValue();
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
	
	
	/* (non-Javadoc)
	 * @see org.jetel.component.RecordTransform#finished()
	 */
	public void finished(){
        // execute finished transformFunction
        if (finishedFunction!=null){
            executor.executeFunction(finishedFunction,null); 
        }
	}
	
    /* (non-Javadoc)
     * @see org.jetel.component.RecordTransform#setGraph(org.jetel.graph.TransformationGraph)
     */
    public void setGraph(TransformationGraph graph) {
        this.graph = graph;
    }
}

