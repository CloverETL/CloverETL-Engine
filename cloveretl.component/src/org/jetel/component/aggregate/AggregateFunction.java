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
package org.jetel.component.aggregate;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.metadata.DataFieldMetadata;

/**
 * Function that can be used in aggregation.
 * 
 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public abstract class AggregateFunction {
	// exception messages for input and output field type checking
	public static final String ERROR_NUMERIC = "must be numeric";
	public static final String ERROR_LONG = "must be long";
	public static final String ERROR_INT = "must be int";
	public static final String ERROR_STRING = "must be string";
	public static final String ERROR_NULLABLE = "must be nullable";
	public static final String ERROR_NULLABLE_BECAUSE_INPUT = "must be nullable because input is nullable";
	public static final String ERROR_OUTPUT_AS_INPUT = "must be the same type as input";
	
	/** Index of the field in input which is used as a parameter for this aggregation function. */
	protected int inputFieldIndex;
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
	 * Re-initialize for use in another aggregation group. Used with sorted input,
	 * where it's not neccessary to create new instances of aggregation functions.
	 *
	 */
	public abstract void clear();

	/**
	 * Update results with a record.
	 * @param record
	 * @throws Exception error occured during update.
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
	 * @throws AggregationException if the input field type has incopatible type; the exception
	 * message should contain a description of the incompatibility (e.g. "must be Numeric").
	 */
	public abstract void checkInputFieldType(DataFieldMetadata inputField) throws AggregationException;
	
	/**
	 * Checks the compatibility of the type of an output field.
	 * @param outputField
	 * @throws AggregationException if the output field type has incopatible type; the exception
	 * message should contain a description of the incompatibility (e.g. "must be Numeric").
	 */
	public abstract void checkOutputFieldType(DataFieldMetadata outputField) throws AggregationException;

	
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
