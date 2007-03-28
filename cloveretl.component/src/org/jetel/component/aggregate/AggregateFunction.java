package org.jetel.component.aggregate;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.metadata.DataFieldMetadata;

/**
 * Function that can be used in aggregation.
 * 
 * @author Jaroslav Urban
 *
 */
public abstract class AggregateFunction {
	/** Name of the field in input which is used as a parameter for this aggregation function. */
	protected String inputFieldName;
	/** Index of the field in input which is used as a parameter for this aggregation function. */
	protected int inputFieldIndex;
	/** Name of the field for output. */
	protected String outputFieldName;
	/** Index of the field for output. */
	protected int outputFieldIndex;
	
	/** Metadata of the field in input which is used as a parameter for this aggregation function. */
	protected DataFieldMetadata inputFieldMetadata;

	/** Is the input data sorted? */
	protected boolean sorted;
	/** Record key. */
	protected RecordKey recordKey;
	/** Charset of input. */
	protected String charset;
	
	/**
	 * 
	 * @return name of the aggregation function; every function must have a unique name. This name
	 * (case-insensitive) is used in the function mapping of the aggregate component. 
	 */
	public abstract String getName();
	
	/**
	 * Initialization of the aggregation function.
	 *
	 */
	public abstract void init();


	/**
	 * Update results with a record.
	 * @param record
	 */
	public abstract void update(DataRecord record) throws Exception;

	/**
	 * Store the result.
	 * @param outputField field for storing the result.
	 */
	public abstract void storeResult(DataField outputField);

	/**
	 * 
	 * @return <tt>true</tt> if the aggregation function requires an input field as a parameter.
	 */
	public abstract boolean requiresInputField();
	
	/**
	 * Checks the compatibility of the type of an input field.
	 * @param inputField
	 * @return <tt>true</tt> if the input field has a type that is compatible with this aggregation
	 * function.
	 */
	public abstract boolean checkInputFieldType(DataFieldMetadata inputField);
	
	/**
	 * Checks the compatibility of the type of an output field.
	 * @param outputField
	 * @return <tt>true</tt> if the output field has a type that is compatible with this aggregation
	 * function.
	 */
	public abstract boolean checkOutputFieldType(DataFieldMetadata outputField);

	
	public String getInputFieldName() {
		return inputFieldName;
	}

	public void setInputFieldName(String inputFieldName) {
		this.inputFieldName = inputFieldName;
	}

	public int getInputFieldIndex() {
		return inputFieldIndex;
	}

	public void setInputFieldIndex(int inputFieldIndex) {
		this.inputFieldIndex = inputFieldIndex;
	}

	public int getOutputFieldIndex() {
		return outputFieldIndex;
	}

	public void setOutputFieldIndex(int outputFieldIndex) {
		this.outputFieldIndex = outputFieldIndex;
	}

	public String getOutputFieldName() {
		return outputFieldName;
	}

	public void setOutputFieldName(String outputFieldName) {
		this.outputFieldName = outputFieldName;
	}

	public DataFieldMetadata getInputFieldMetadata() {
		return inputFieldMetadata;
	}

	public void setInputFieldMetadata(DataFieldMetadata inputFieldMetadata) {
		this.inputFieldMetadata = inputFieldMetadata;
	}

	public RecordKey getRecordKey() {
		return recordKey;
	}

	public void setRecordKey(RecordKey recordKey) {
		this.recordKey = recordKey;
	}

	public boolean isSorted() {
		return sorted;
	}

	public void setSorted(boolean sorted) {
		this.sorted = sorted;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	
}
