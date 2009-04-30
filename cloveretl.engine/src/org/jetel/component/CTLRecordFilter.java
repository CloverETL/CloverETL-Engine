package org.jetel.component;

import org.jetel.ctl.CTLCompilable;
import org.jetel.ctl.CTLEntryPoint;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;

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
	public abstract boolean validDelegate();
	
	
	public boolean isValid(DataRecord record) {
		inputRecord = record;
		return validDelegate();
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
