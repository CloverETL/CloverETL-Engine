package org.jetel.component;

import org.jetel.ctl.CTLCompilable;
import org.jetel.ctl.CTLEntryPoint;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;

public abstract class CTLRecordFilter implements RecordFilter, CTLCompilable {

	private DataRecord inputRecord = null;
	
	@CTLEntryPoint(
			name = "init",
			required = false
	)
	public void init() throws ComponentNotReadyException {
		global();
	}
	

	@CTLEntryPoint(
			name = CTLRecordFilterAdapter.ISVALID_FUNCTION_NAME,
			required = true
	)
	public abstract boolean validDelegate() throws ComponentNotReadyException, TransformException;
	
	
	public boolean isValid(DataRecord record) throws TransformException {
		inputRecord = record;
		try {
			return validDelegate();
		} catch (ComponentNotReadyException e) {
			// the exception may be thrown by lookups etc...
			throw new TransformException("Generated transformation class threw an exception",e);
		}
	}

	
	
	public void global() throws ComponentNotReadyException {
		// do nothing by default
	}
	
	public DataRecord getInputRecord(int index) {
		return index == 0 ? inputRecord : null;
	}
	
	public DataRecord getOutputRecord(int index) {
		return null;
	}

}
