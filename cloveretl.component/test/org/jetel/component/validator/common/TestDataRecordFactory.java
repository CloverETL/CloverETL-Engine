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
package org.jetel.component.validator.common;

import java.util.Date;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.primitive.Decimal;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordParsingType;

/**
 * Factory for "mocking" DataRecord objects for testing Validator and its rules.
 * It can easily add fields and its content. 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 11.12.2012
 * @see ValidatorTestCase and its childs
 */
public class TestDataRecordFactory {
	private static int metadataCounter = 0;
	
	/**
	 * Creates new empty data record. Named automatically
	 * @return Empty data record
	 */
	public static DataRecord newRecord() {
		DataRecordMetadata metadata = new DataRecordMetadata("TestMetadataNumber" + ++metadataCounter, DataRecordParsingType.DELIMITED);
		return DataRecordFactory.newRecord(metadata);
	}
	
	/**
	 * Creates copy of given record extended with new field.
	 * @param record Record to extend
	 * @param newFieldType Date type to extend
	 * @param newFieldName Name of field to extend
	 * @param newFieldValue Value of field to insert
	 * @return Extended record
	 */
	private static DataRecord extendRecord(DataRecord record, DataFieldType newFieldType, String newFieldName, Object newFieldValue) {
		if(record == null) {
			record = newRecord();
		}
		DataRecordMetadata newMetadata = record.getMetadata();
		DataFieldMetadata fieldMetadata = new DataFieldMetadata(newFieldName, newFieldType, ";");
		newMetadata.addField(fieldMetadata);
		DataRecord out = DataRecordFactory.newRecord(newMetadata); 
		out.init();
		out.copyFrom(record);
		out.getField(newFieldName).setValue(newFieldValue);
		return out;
	}
	
	/**
	 * Adds string field to given record
	 * @param record Record to extend
	 * @param name Name of field
	 * @param value Value of field
	 * @return Extended record
	 */
	public static DataRecord addStringField(DataRecord record, String name, String value) {
		return extendRecord(record, DataFieldType.STRING, name, value);
	}
	
	/**
	 * Adds integer field to given record
	 * @param record Record to extend
	 * @param name Name of field
	 * @param value Value of field
	 * @return Extended record
	 */
	public static DataRecord addIntegerField(DataRecord record, String name, Integer value) {
		return extendRecord(record, DataFieldType.INTEGER, name, value);
	}

	/**
	 * Adds long field to given record
	 * @param record Record to extend
	 * @param name Name of field
	 * @param value Value of field
	 * @return Extended record
	 */
	public static DataRecord addLongField(DataRecord record, String name, Long value) {
		return extendRecord(record, DataFieldType.LONG, name, value);
	}
	
	/**
	 * Adds boolean field to given record
	 * @param record Record to extend
	 * @param name Name of field
	 * @param value Value of field
	 * @return Extended record
	 */
	public static DataRecord addBooleanField(DataRecord record, String name, Boolean value) {
		return extendRecord(record, DataFieldType.BOOLEAN, name, value);
	}
	
	/**
	 * Adds number field to given record
	 * @param record Record to extend
	 * @param name Name of field
	 * @param value Value of field
	 * @return Extended record
	 */
	public static DataRecord addNumberField(DataRecord record, String name, Double value) {
		return extendRecord(record, DataFieldType.NUMBER, name, value);
	}
	
	/**
	 * Adds date field to given record
	 * @param record Record to extend
	 * @param name Name of field
	 * @param value Value of field
	 * @return Extended record
	 */
	public static DataRecord addDateField(DataRecord record, String name, Date value) {
		return extendRecord(record, DataFieldType.DATE, name, value);
	}
	
	/**
	 * Adds decimal field to given record
	 * @param record Record to extend
	 * @param name Name of field
	 * @param value Value of field
	 * @return Extended record
	 */
	public static DataRecord addDecimalField(DataRecord record, String name, Decimal value) {
		return extendRecord(record, DataFieldType.DECIMAL, name, value);
	}
	
}
