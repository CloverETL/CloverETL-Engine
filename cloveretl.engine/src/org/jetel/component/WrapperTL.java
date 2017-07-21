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
package org.jetel.component;

import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.TransformationGraph;
import org.jetel.interpreter.ParseException;
import org.jetel.interpreter.TransformLangExecutor;
import org.jetel.interpreter.TransformLangParser;
import org.jetel.interpreter.ASTnode.CLVFFunctionDeclaration;
import org.jetel.interpreter.ASTnode.CLVFStart;
import org.jetel.interpreter.data.TLValue;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * This class is used for executing code written in CloverETL transform language.
 * Before executing it is necessary to call init() method (usually when calling 
 * init() function)
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Dec 4, 2006
 *
 */
@SuppressWarnings("EI2")
public class WrapperTL {

    public static final String TL_TRANSFORM_CODE_ID="//#TL";  // magic header determining that the source code is Clover's TransformLanguage
    public static final String TL_TRANSFORM_CODE_ID2="//#CTL1";  // magic header determining that the source code is Clover's TransformLanguage
    
    private String srcCode;
    private Log logger;
    private Properties parameters;
    private DataRecordMetadata[] sourceMetadata;
    private DataRecordMetadata[] targetMetadata;
    
    private TransformLangParser parser;
    private TransformLangExecutor executor;

    private int functionCounter = 0;
    private int functionNumber = 10;
    private CLVFFunctionDeclaration[] function = new CLVFFunctionDeclaration[functionNumber];
    
    
    private String errorMessage;
    private TransformationGraph graph;
    
	/**
	 * Constructor
	 * 
	 * @param srcCode code written in CloverETL language
	 * @param sourceMetadata
	 * @param targetMetadata
	 * @param parameters transformation parameters
	 * @param logger
	 */
	public WrapperTL(String srcCode, DataRecordMetadata[] sourceMetadata,
			DataRecordMetadata[] targetMetadata, Properties parameters, Log logger) {
		this.srcCode = srcCode;
		this.logger = logger;
		this.parameters = parameters;
		this.sourceMetadata = sourceMetadata;
		this.targetMetadata = targetMetadata;
	}
    
	/**
	 * @param srcCode code written in CloverETL language
	 * @param sourceMetadata
	 * @param targetMetadata
	 * @param logger
	 */
	public WrapperTL(String srcCode, DataRecordMetadata[] sourceMetadata,
			DataRecordMetadata[] targetMetadata, Log logger) {
		this(srcCode, sourceMetadata, targetMetadata, null, logger);
	}

	/**
	 * @param srcCode code written in CloverETL language
	 * @param metadata when output and input metadata are identical or only one needed
	 * @param parameters transformation parameters
	 * @param logger
	 */
	public WrapperTL(String srcCode, DataRecordMetadata metadata,
			Properties parameters, Log logger) {
		this(srcCode,new DataRecordMetadata[]{metadata}, 
				new DataRecordMetadata[]{metadata}, parameters, logger);
	}

	
	/**
	 * @param srcCode code written in CloverETL language
	 * @param parameters transformation parameters
	 * @param logger
	 */
	public WrapperTL(String srcCode, Properties parameters, Log logger) {
		this(srcCode,null, null, parameters, logger);
	}

	/**
	 * @param srcCode code written in CloverETL language
	 * @param metadata when output and input metadata are identical or only one needed
	 * @param logger
	 */
	public WrapperTL(String srcCode, DataRecordMetadata metadata, Log logger) {
		this(srcCode,new DataRecordMetadata[]{metadata}, 
				new DataRecordMetadata[]{metadata}, null, logger);
	}

	
	/**
	 * @param srcCode code written in CloverETL language
	 * @param logger
	 */
	public WrapperTL(String srcCode, Log logger) {
		this(srcCode,null, null, null, logger);
	}

	/**
	 * This method parses source code written in CloverETL language and initializes
	 * 	CloverETL language executor. It has to be called before executing any function
	 * 
	 * @throws ComponentNotReadyException
	 */
	public void init() throws ComponentNotReadyException{
        CLVFStart parseTree=null;
        //creating parser
		parser = new TransformLangParser(sourceMetadata, targetMetadata,srcCode); 
		parser.setProjectURL(getGraph().getRuntimeContext().getContextURL());
		//initializing parser
        try {
            parseTree = parser.Start();
            parseTree.init();
        }catch(ParseException ex){
            errorMessage = ExceptionUtils.getMessage(ex);
            throw new ComponentNotReadyException(ex);
        }catch(Exception ex){
            errorMessage = ExceptionUtils.getMessage(ex);
            throw new ComponentNotReadyException(ex);
        }
        
        // log & report any parse exceptions
        for(Iterator<?> iter=parser.getParseExceptions().iterator();iter.hasNext();){
            logger.error(iter.next());
        }
        
        if (parser.getParseExceptions().size()>0){
            throw new ComponentNotReadyException(((Exception)parser.getParseExceptions().get(0)));
        }
        
        //initializing executor
        executor=new TransformLangExecutor(parameters);
        executor.setRuntimeLogger(logger);
        executor.setParser(parser);
		if (parameters != null) {
			executor.setGlobalParameters(parameters);
		}		
		if (graph != null) {
			executor.setGraph(graph);
		}		
		// execute global declarations, etc
        try{
            executor.visit(parseTree,null);
        }catch (Exception ex){
            errorMessage = ExceptionUtils.getMessage(ex);
            throw new ComponentNotReadyException(ex);
        }
    	
    }
	
	/**
	 * This method executes function wriiten in CloverETL language
	 * 
	 * @param functionName function name
	 * @param inputRecords input records 
	 * @param outputRecords output records 
	 * @return result of executing function
	 * @throws JetelException
	 */
	public TLValue execute(String functionName, DataRecord[] inputRecords, 
			DataRecord[] outputRecords, TLValue[] data) throws JetelException{
		CLVFFunctionDeclaration function = (CLVFFunctionDeclaration)parser.getFunctions().get(functionName);
		if (function == null) {//function with given name not found
			throw new JetelException("Function " + functionName + " not found");
		}
		//set input and output records (if given)
		if (inputRecords != null) {
			executor.setInputRecords(inputRecords);
		}		
		if (outputRecords != null){
			executor.setOutputRecords(outputRecords);
		}
		//execute function
		executor.executeFunction(function,data);
        //return result
        return executor.getResult();
	}
	
	/**
	 * This method executes function wriiten in CloverETL language
	 * 
	 * @param functionName
	 * @param inRecord
	 * @return
	 * @throws JetelException
	 */
	public TLValue execute(String functionName, DataRecord inRecord, TLValue[] data)
			throws JetelException{
		return execute(functionName, new DataRecord[]{inRecord}, null, data);
	}
	
	/**
	 * This method executes function wriiten in CloverETL language
	 * 
	 * @param functionName
	 * @param inRecord
	 * @return
	 * @throws JetelException
	 */
	public TLValue execute(String functionName, TLValue[] data)throws JetelException{
		return execute(functionName, null, null, data);
	}

	/**
	 * If there is expected that the function will be executed many times, it is 
	 * possible to set this function as "default" for quicker execution
	 * 
	 * @param functionName name of function, which will be executed many times
	 * @throws ComponentNotReadyException
	 */
	public int prepareFunctionExecution(String functionName) throws ComponentNotReadyException {
		int functionId = prepareOptionalFunctionExecution(functionName);

		if (functionId >= 0) {
			return functionId;
		}

		throw new ComponentNotReadyException("Function " + functionName + " not found");
	}
	
    /**
     * Prepares an optional function for execution.
     *
     * @param functionName the name of the function to be prepared
     *
     * @return an integer identification of the function if declared, -1 otherwise
     */
	public int prepareOptionalFunctionExecution(String functionName) {
		if (functionCounter + 1 > functionNumber) {
			functionNumber *= 2;
			CLVFFunctionDeclaration[] tmp = function;
			function = new CLVFFunctionDeclaration[functionNumber];
			System.arraycopy(tmp, 0, function, 0, tmp.length);
		}

		function[functionCounter] = (CLVFFunctionDeclaration) parser.getFunctions().get(functionName);

		if (function[functionCounter] != null) {
			return functionCounter++;
		}

		//function with given name not found
		return -1;
	}

	/**
	 * This method exexutes function set before as default (see above)
	 * 
	 * @param functionNumber number of function
	 * @param inputRecords
	 * @param outputRecords
	 * @param data function parameters
	 * @return
	 */
	public TLValue executePreparedFunction(int functionNumber, 
			DataRecord[] inputRecords, DataRecord[] outputRecords, TLValue[] data){
		//set input and output records (if given)
		if (inputRecords != null) {
			executor.setInputRecords(inputRecords);
		}		
		if (outputRecords != null){
			executor.setOutputRecords(outputRecords);
		}
		//execute function
		executor.executeFunction(function[functionNumber],data);
        //return result
        return executor.getResult();
	}

	/**
	 * This method exexutes 0th function set before as default (see above)
	 * 
	 * @param inputRecords
	 * @param outputRecords
	 * @param data function parameters
	 * @return
	 */
	public TLValue executePreparedFunction(DataRecord[] inputRecords, 
			DataRecord[] outputRecords, TLValue[] data){
		//set input and output records (if given)
		if (inputRecords != null) {
			executor.setInputRecords(inputRecords);
		}		
		if (outputRecords != null){
			executor.setOutputRecords(outputRecords);
		}
		//execute function
		executor.executeFunction(function[0],data);
        //return result
        return executor.getResult();
	}

	/**
	 * This method executes function set before as default (see above)
	 * 
	 * @param inRecord
	 * @return
	 */
	public TLValue executePreparedFunction(int functionNumber, DataRecord inRecord,
			TLValue[] data){
		return executePreparedFunction(functionNumber, 
				new DataRecord[]{inRecord}, null, data);
	}
	
	/**
	 * This method exexutes 0th function set before as default (see above)
	 * 
	 * @param inRecord
	 * @return
	 */
	public TLValue executePreparedFunction(DataRecord inRecord, TLValue[] data){
		return executePreparedFunction(new DataRecord[]{inRecord}, null, data);
	}

	public TLValue executePreparedFunction(int functionNumber, TLValue[] data){
		return executePreparedFunction(functionNumber, null, null, data);
	}

	public TLValue executePreparedFunction(TLValue[] data){
		return executePreparedFunction(null, null, data);
	}

	/**
	 * This method executes function set before as default (see above)
	 * 
	 * @return
	 */
	public TLValue executePreparedFunction(int functionNumber){
		return executePreparedFunction(functionNumber, null, null);
	}
	
	/**
	 * This method executes 0th function set before as default (see above)
	 * 
	 * @return
	 */
	public TLValue executePreparedFunction(){
		return executePreparedFunction(null, null);
	}

	/**
	 * @return transformation graph
	 */
	public TransformationGraph getGraph() {
		return graph;
	}

	/**
	 * Sets transformation graph
	 * 
	 * @param graph
	 */
	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}

	/**
	 * @return error messages
	 */
	public String getErrorMessage() {
		return errorMessage;
	}

	/**
	 * Sets transformation parameters
	 * 
	 * @param parameters
	 */
	public void setParameters(Properties parameters) {
		this.parameters = parameters;
	}
	
	public void setMetadata(DataRecordMetadata[] sourceMetadata, DataRecordMetadata[] targetMetadata){
		this.sourceMetadata = sourceMetadata;
		this.targetMetadata = targetMetadata;
	}

	public void setMatadata(DataRecordMetadata metadata){
		this.sourceMetadata = new DataRecordMetadata[]{metadata};
		this.targetMetadata = new DataRecordMetadata[]{metadata};
	}

	/**
	 * Resets this instance for next graph execution. 
	 */
	public void reset() {
		errorMessage = null;
	}
}
