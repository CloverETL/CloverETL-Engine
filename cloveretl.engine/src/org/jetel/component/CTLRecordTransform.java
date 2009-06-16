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
 * Ancestor for all Java code generated from CTL programs in Reformat-like and Joiner-like components.
 * 
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 *
 */
public abstract class CTLRecordTransform implements RecordTransform, CTLCompilable {

	private DataRecord[] inputRecords;
	private DataRecord[] outputRecord;

	/**
	 * Initializes global scope via call to {@link #global()}, the calls user initialization code
	 * via {@link #init()} method.
	 * 
	 * All parameters of the call are ignored.
	 * 
	 * @return True if successfull, otherwise False
	 */
	public boolean init(Properties parameters, DataRecordMetadata[] sourceRecordsMetadata,
			DataRecordMetadata[] targetRecordsMetadata) throws ComponentNotReadyException {

		global();
		return init();
	}

	/**
	 * Method to be overridden by user. It is called by init(Properties,DataRecordMetadata[],DataRecordMetadata[]) method
	 * when all standard initialization is performed.<br>
	 * This implementation is just skeleton. User should provide its own.
	 * 
	 * @return true if user's initialization was performed successfully
	 */
	@CTLEntryPoint(
			required = false,
			name = "init"
	)
	public boolean init() throws ComponentNotReadyException {
		return true;
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
	public void global() throws org.jetel.exception.ComponentNotReadyException {
		// by default do nothing
	}
	/**
	 * Transforms source data records into target data records. Derived class should perform this functionality.<br>
	 * This basic version only copies content of inputRecord into outputRecord field by field. See
	 * DataRecord.copyFieldsByPosition() method.
	 * 
	 * @param sourceRecords
	 *            input data records (an array)
	 * @param targetRecords
	 *            output data records (an array)
     * @return RecordTransform.ALL -- send the data record(s) to all the output ports<br>
     *         RecordTransform.SKIP -- skip the data record(s)<br>
     *         >= 0 -- send the data record(s) to a specified output port<br>
     *         < -1 -- fatal error / user defined
	 * @see org.jetel.data.DataRecord#copyFieldsByPosition()
	 */
	@CTLEntryPoint(
			required = true,
			name = "transform"
	)
	public abstract int transformDelegate() throws TransformException, ComponentNotReadyException;

	/**
	 * Implementation of interface function {@link #transform(DataRecord[], DataRecord[])}
	 */
	public int transform(DataRecord[] sources, DataRecord[] targets) throws TransformException {
		inputRecords = sources;
		outputRecord = targets;
		try {
			return transformDelegate();
		} catch (ComponentNotReadyException e) {
			// the exception may be thrown by lookups etc...
			throw new TransformException("Generated transformation class threw an exception",e);
		}
	}
	
	/**
	 * Returns description of error if one of the methods failed
	 * 
	 * @return Error message
	 * @since April 18, 2002
	 */
	public String getMessage() {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.component.RecordTransform#signal() In this implementation does nothing.
	 */
	public void signal(Object signalObject) {

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.component.RecordTransform#getSemiResult()
	 */
	public Object getSemiResult() {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.component.RecordTransform#finished()
	 */
	@CTLEntryPoint(
			required = false,
			name = "finished"
	)
	public void finished() {

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.component.RecordTransform#reset()
	 */
	public void reset() {
		// by default do nothing
	}
	
	public TransformationGraph getGraph() {
		// not used by transformations
		return null;
	}
	
	public DataRecord getInputRecord(int index) {
		return inputRecords[index];
	}
	
	public DataRecord getOutputRecord(int index) {
		return outputRecord[index];
	}

}
