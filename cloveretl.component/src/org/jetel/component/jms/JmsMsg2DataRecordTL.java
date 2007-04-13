/**
 * 
 */
package org.jetel.component.jms;

import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.commons.logging.Log;
import org.jetel.component.WrapperTL;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author avackova
 *
 */
public class JmsMsg2DataRecordTL implements JmsMsg2DataRecord {
	
	private WrapperTL wrapper;

    private static final String INIT_FUNCTION_NAME="init";
    public static final String FINISHED_FUNCTION_NAME="finished";
    private static final String END_FUNCTION_NAME="endOfInput";
    private static final String EXTRACT_FUNCTION_NAME="extractRecord";
    
    private int endFunctionIdentifier;
    private int exctractFunctionIdentifier;
	private DataRecord record;    
    
	public JmsMsg2DataRecordTL(Log logger,String srcCode) {
		 wrapper = new WrapperTL(srcCode,logger);
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.jms.JmsMsg2DataRecord#endOfInput()
	 */
	public boolean endOfInput() {
		Object result = wrapper.executePreparedFunction(endFunctionIdentifier);
		return result == null ? true : (Boolean)result;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.jms.JmsMsg2DataRecord#extractRecord(javax.jms.Message)
	 */
	public DataRecord extractRecord(Message msg) throws JMSException {
		wrapper.executePreparedFunction(exctractFunctionIdentifier, 
				new DataRecord[]{record}, new DataRecord[]{record}, new Object[]{msg});
		return record;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.jms.JmsMsg2DataRecord#finished()
	 */
	public void finished() {
        // execute finished transformFunction
		try {
			wrapper.execute(FINISHED_FUNCTION_NAME,null);
		} catch (JetelException e) {
			//do nothing: function finished is not necessary
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.jms.JmsMsg2DataRecord#getErrorMsg()
	 */
	public String getErrorMsg() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.jms.JmsMsg2DataRecord#init(org.jetel.metadata.DataRecordMetadata, java.util.Properties)
	 */
	public void init(DataRecordMetadata metadata, Properties props)
			throws ComponentNotReadyException {
		wrapper.setMatadata(metadata);
		wrapper.setParameters(props);
		wrapper.init();
		try {
			wrapper.execute(INIT_FUNCTION_NAME,null);
		} catch (JetelException e) {
			//do nothing: function init is not necessary
		}
		
		record = new DataRecord(metadata);
		record.init();
		
		exctractFunctionIdentifier = wrapper.prepareFunctionExecution(EXTRACT_FUNCTION_NAME);
		endFunctionIdentifier = wrapper.prepareFunctionExecution(END_FUNCTION_NAME);
		
	}

}
