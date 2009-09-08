package org.jetel.component;

import java.util.Properties;

import org.jetel.ctl.CTLCompilable;
import org.jetel.ctl.CTLEntryPoint;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

/**
 * 
 * Base class for source code generated from CTL scripts in DataGenerator component.
 * 
 * @author Michal Tomcanyi
 *
 */
public abstract class CTLRecordGenerate implements RecordGenerate, CTLCompilable {

	private DataRecord[] outputRecord;

	@CTLEntryPoint(
			required = false,
			name = "init"
	)
	protected boolean init() throws ComponentNotReadyException {
		return true;
	}
	
	
	public boolean init(Properties parameters, DataRecordMetadata[] targetMetadata) throws ComponentNotReadyException {
		global();
		return init();
	}
	
	
	
	
	/**
	 * Method for CTL compilation. Contains code from global scope
	 * and global variables initialization.
	 * 
	 * @param in	input records
	 * @param out	output records
	 */
	@CTLEntryPoint(
			required = false,
			name = "global"
	)
	public void global() throws ComponentNotReadyException {
		// by default do nothing
	}
	
	
	@CTLEntryPoint(
			required = true,
			name = "generate"
	)
	public abstract int generateDelegate() throws TransformException;
	
	
	
	public int generate(DataRecord[] target) throws TransformException {
		outputRecord = target;
		return generateDelegate();
	}
	
		
	public void finished() {
		// nothing to do
	}

	public TransformationGraph getGraph() {
		// not used by transformations
		return null;
	}

	public String getMessage() {
		// nothing to return
		return null;
	}

	public Object getSemiResult() {
		// not used
		return null;
	}

	

	public void reset() {
		// by default do nothing
		
	}

	public void setGraph(TransformationGraph graph) {
		// by default do nothing
		
	}

	public void signal(Object signalObject) {
		// by default do nothing
	}

	public DataRecord getInputRecord(int index) {
		return null;
	}

	public DataRecord getOutputRecord(int index) {
		return outputRecord[index];
	}


}
