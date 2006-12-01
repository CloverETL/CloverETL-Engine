
package org.jetel.component;

import java.io.ByteArrayInputStream;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.primitive.CloverInteger;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.interpreter.ParseException;
import org.jetel.interpreter.TransformLangExecutor;
import org.jetel.interpreter.TransformLangParser;
import org.jetel.interpreter.node.CLVFFunctionDeclaration;
import org.jetel.interpreter.node.CLVFStart;
import org.jetel.metadata.DataRecordMetadata;

public class WrapperTL {

    public static final String TL_TRANSFORM_CODE_ID="//#TL";  // magic header determining that the source code is Clover's TransformLanguage
    
    private String srcCode;
    private Log logger;
    private Properties parameters;
    private DataRecordMetadata[] sourceMetadata;
    private DataRecordMetadata[] targetMetadata;
    
    private TransformLangParser parser;
    private TransformLangExecutor executor;
    
    private String errorMessage;
    
	/**
	 * @param srcCode
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
    
	public WrapperTL(String srcCode, DataRecordMetadata[] sourceMetadata,
			DataRecordMetadata[] targetMetadata, Log logger) {
		this(srcCode, sourceMetadata, targetMetadata, null, logger);
	}

	public WrapperTL(String srcCode, DataRecordMetadata metadata,
			Properties parameters, Log logger) {
		this(srcCode,new DataRecordMetadata[]{metadata}, 
				new DataRecordMetadata[]{metadata}, parameters, logger);
	}

	
	public WrapperTL(String srcCode, Properties parameters, Log logger) {
		this(srcCode,null, null, parameters, logger);
	}

	public WrapperTL(String srcCode, DataRecordMetadata metadata, Log logger) {
		this(srcCode,new DataRecordMetadata[]{metadata}, 
				new DataRecordMetadata[]{metadata}, null, logger);
	}

	
	public WrapperTL(String srcCode, Log logger) {
		this(srcCode,null, null, null, logger);
	}

	public void init() throws ComponentNotReadyException{
        CLVFStart parseTree=null;
        if (sourceMetadata != null) {
        	parser = new TransformLangParser(sourceMetadata, targetMetadata, 
        			new ByteArrayInputStream(srcCode.getBytes()));
        }else{
        	parser = new TransformLangParser(new ByteArrayInputStream(srcCode.getBytes()));
        }
        try {
            parseTree = parser.Start();
            parseTree.init();
        }catch(ParseException ex){
            ex.printStackTrace();
            errorMessage = ex.getLocalizedMessage();
            logger.error(ex);
            throw new ComponentNotReadyException(ex);
        }catch(Exception ex){
            ex.printStackTrace();
            errorMessage = ex.getLocalizedMessage();
            logger.error(ex);
            throw new ComponentNotReadyException(ex);
        }
        
        // log & report any parse exceptions
        for(Iterator iter=parser.getParseExceptions().iterator();iter.hasNext();){
            logger.error(iter.next());
        }
        
        if (parser.getParseExceptions().size()>0){
            errorMessage=((Exception)parser.getParseExceptions().get(0)).getMessage();
            logger.error(errorMessage);
            throw new ComponentNotReadyException(errorMessage);
        }
        
        executor=new TransformLangExecutor(parameters);
       
        // execute global declarations, etc
        try{
            executor.visit(parseTree,null);
        }catch (Exception ex){
            logger.error(ex);
            errorMessage=ex.getMessage();
            throw new ComponentNotReadyException(errorMessage);
        }
    	
    }
	
	public Object execute(String functionName, DataRecord[] inputRecords, 
			DataRecord[] outputRecords){
		CLVFFunctionDeclaration function = (CLVFFunctionDeclaration)parser.getFunctions().get(functionName);
		if (function == null) {
			return null;
		}
		if (inputRecords != null) {
			executor.setInputRecords(inputRecords);
		}		
		if (outputRecords != null){
			executor.setOutputRecords(outputRecords);
		}
		executor.executeFunction(function,null);
        
        Object result = executor.getResult();
        if (result!=null){
            return result;
        }
        return true;
	}
	
	public Object execute(String functionName, DataRecord inRecord){
		return execute(functionName, new DataRecord[]{inRecord}, null);
	}
	
	public Object execute(String functionName){
		return execute(functionName, null);
	}

}
