/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.component.aggregate;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.Number;
import org.jetel.data.RecordKey;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  Represent aggregate function of agregate component.
 *
 * @author      Martin Zatopek, OpenTech, s.r.o (www.opentech.cz)
 * @since       June 27, 2005
 * @revision    $Revision$
 */
public class AggregateFunction implements Iterator {

	final static int AGGREGATE_GROUP_INITIAL_CAPACITY = 512;		

	private final static int FNC_MIN = 0;
	private final static int FNC_MAX = 1;
	private final static int FNC_SUM = 2;
	private final static int FNC_COUNT = 3;
	private final static int FNC_AVG = 4;
	private final static int FNC_STDEV = 5;

	private final static String S_FNC_MIN = "MIN";
	private final static String S_FNC_MAX = "MAX";
	private final static String S_FNC_SUM = "SUM";
	private final static String S_FNC_COUNT = "COUNT";
	private final static String S_FNC_AVG = "AVG";
	private final static String S_FNC_STDEV = "STDEV";

	private DataRecordMetadata inMetadata;
	private DataRecordMetadata outMetadata;
	private AggregateItem[] aggregateItems;
	private String aggregateFunction;
	private RecordKey recordKey;
	private int aggregateGroupInitialCapacity;
	private boolean sorted;
	private DataRecord outRecord;
	private Map recordMap;
	
	/**
	 *Constructor for the AggregateFunction object
	 */
	public AggregateFunction(String aggregateFunction, DataRecordMetadata inRecordMetadata, DataRecordMetadata outRecordMetadata, RecordKey recordKey, boolean sorted, int aggregateGroupInitialCapacity) {
		this.aggregateFunction = aggregateFunction;
		this.recordKey = recordKey;
		inMetadata = inRecordMetadata;
		outMetadata = outRecordMetadata;
		this.sorted = sorted;
		this.aggregateGroupInitialCapacity = aggregateGroupInitialCapacity;
	}

	/**
	 *Constructor for the AggregateFunction object
	 */
	public AggregateFunction(String aggregateFunction, DataRecordMetadata inRecordMetadata, DataRecordMetadata outRecordMetadata, RecordKey recordKey, boolean sorted) {
		this(aggregateFunction, inRecordMetadata, outRecordMetadata, recordKey, sorted, AGGREGATE_GROUP_INITIAL_CAPACITY);
	}

	/**
	 *  Description of the Method
	 *
	 * @param  recordMetadata  Description of the Parameter
	 * @param  recordMetadata  Description of the Parameter
	 */
	public void init() {
		int tempIndex;
		int functionNumber;
		String functionPart;
		String functionName;
		String functionParameter;
		String[] functionParts = aggregateFunction.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		DataFieldMetadata fieldMetadata;
		Integer fieldNum;
		
		aggregateItems = new AggregateItem[functionParts.length];
		Map fieldNames = inMetadata.getFieldNames();

		for (int i = 0; i < functionParts.length; i++) {
			functionPart = functionParts[i];
			
			//what function?
			if((tempIndex = functionPart.indexOf("(")) == -1) {
				throw new RuntimeException("Invalid aggragate function definition.");
			}
			functionName = functionPart.substring(0, tempIndex).trim();
			
			if(functionName.equalsIgnoreCase(S_FNC_MIN)) {
				functionNumber = FNC_MIN;
			} else if(functionName.equalsIgnoreCase(S_FNC_MAX)) {
				functionNumber = FNC_MAX;
			} else if(functionName.equalsIgnoreCase(S_FNC_SUM)) {
				functionNumber = FNC_SUM;
			} else if(functionName.equalsIgnoreCase(S_FNC_COUNT)) {
				functionNumber = FNC_COUNT;
			} else if(functionName.equalsIgnoreCase(S_FNC_AVG)) {
				functionNumber = FNC_AVG;
			} else if(functionName.equalsIgnoreCase(S_FNC_STDEV)) {
				functionNumber = FNC_STDEV;
			} else {
				throw new RuntimeException("Unknown aggregate function: " + functionName);
			}
			
			if(functionNumber == FNC_COUNT) { // parameter for COUNT aggregate function is not meanfull
				aggregateItems[i] = new AggregateItem(functionNumber, -1, ' ');
				continue;
			}
			
			//what parameter of function
			if((tempIndex = functionPart.indexOf(")")) == -1) {
				throw new RuntimeException("Invalid aggragate function definition.");
			}
			functionParameter = functionPart.substring(functionPart.indexOf("(") + 1, tempIndex).trim();
			
			fieldMetadata = inMetadata.getField(functionParameter);
			if (fieldMetadata == null) {
				throw new RuntimeException("Unknown field name: " + functionParameter);
			}

			fieldNum = (Integer) fieldNames.get(functionParameter);
			if (fieldNum == null) {
				throw new RuntimeException("Unknown field name: " + functionParameter);
			}

			//create aggregate item
			if(inMetadata.getField(fieldNum.intValue()).getType() != DataFieldMetadata.INTEGER_FIELD
					&& inMetadata.getField(fieldNum.intValue()).getType() != DataFieldMetadata.NUMERIC_FIELD
					&& functionNumber != FNC_MIN
					&& functionNumber != FNC_MAX
					&& functionNumber != FNC_COUNT) {
				throw new RuntimeException("Incorrect data type for aggregation.");
			}
			aggregateItems[i] = new AggregateItem(functionNumber, fieldNum.intValue(), inMetadata.getField(fieldNum.intValue()).getType());
		}
		
		if(!sorted) {
			recordMap = new HashMap(AGGREGATE_GROUP_INITIAL_CAPACITY);
		}
	}
	
	/**
	 * Add record for sorted data.
	 * @param record record
	 */
	public void addSortedRecord(DataRecord record) {
		for (int i = 0; i < aggregateItems.length; i++) {
			aggregateItems[i].update(record);
		}
	}

	/**
	 * Gets output record for sorted data.
	 * @return output record
	 */
	public DataRecord getRecordForGroup(DataRecord inRecord, DataRecord outRecord) {
		int[] keyFields = recordKey.getKeyFields();
		
		//copy key to output record
		for (int i = 0; i < keyFields.length; i++) {
			outRecord.getField(i).copyFrom(inRecord.getField(keyFields[i]));
		}
		
		//copy agregation results
		for (int i = 0; i < aggregateItems.length; i++) {
			outRecord.getField(keyFields.length + i).setValue(aggregateItems[i].getValue());
		}

		return outRecord;
	}
	
	/**
	 * Adds record to data structure for unsorted input data.
	 */
	public void addUnsortedRecord(DataRecord record) {
		String keyString;
		for (int i = 0; i < aggregateItems.length; i++) {
			aggregateItems[i].updateUnsorted(record);
			
			keyString = recordKey.getKeyString(record);
			if(!recordMap.containsKey(keyString)) {
				recordMap.put(keyString, record.duplicate());
			}
		}
	}
	
	private Iterator i; 
	/**
	 * Gets output records for unsorted data.
	 * @param outRecord record is used for output record of group
	 */
	public Iterator iterator(DataRecord outRecord) {
		if(aggregateItems.length == 0 || sorted) {
			throw new RuntimeException("Incorrect call of this method.");
		}

		this.outRecord = outRecord;
		i = aggregateItems[0].dataMap.keySet().iterator();

		return this;
	}
	
	/* (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	public void remove() {
		throw new RuntimeException("Call of unimplented method.");
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	public boolean hasNext() {
		return i.hasNext();
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	public Object next() {
		int[] keyFields = recordKey.getKeyFields();
		
		String key = (String) i.next();
		
		//copy key to output record
		for (int i = 0; i < keyFields.length; i++) {
			outRecord.getField(i).copyFrom(((DataRecord) recordMap.get(key)).getField(keyFields[i]));
		}
		
		//copy agregation results
		for (int i = 0; i < aggregateItems.length; i++) {
			outRecord.getField(keyFields.length + i).setValue(aggregateItems[i].getValueUnsorted(key));
		}
		
		return outRecord;
	}

	/**
	 *  Represent one aggregate function.
	 *
	 * @author      Martin Zatopek, OpenTech, s.r.o (www.opentech.cz)
	 * @since
	 * @revision    $Revision$
	 */
	private class AggregateItem {
		boolean firstLoop;
		int function;
		int fieldNo;
		char fieldType;
		Data data;
		Map dataMap; 

		/**
		 *Constructor for the FilterItem object
		 *
		 * @param  function   Description of the Parameter
		 * @param  fieldNo    Description of the Parameter
		 */
		AggregateItem(int function, int fieldNo, char fieldtype) {
			this.function = function;
			this.fieldNo = fieldNo;
			this.fieldType = fieldtype;
			
			firstLoop = true;
			
			if(!sorted) {
				dataMap = new HashMap(aggregateGroupInitialCapacity);
				
				if (dataMap == null) {
					throw new RuntimeException("Can't allocate HashSet of size: " + aggregateGroupInitialCapacity);
				}
			}
		}

		/**
		 * Update this aggregate item with record.
		 * @param record record
		 */
		void update(DataRecord record) {
			DataField currentDF = null;
			Object currentValue = null;
			if(function != FNC_COUNT) {
				currentDF = record.getField(fieldNo);
				currentValue = currentDF.getValue();
				if(currentValue == null) return;
 			}
			if(data == null) {
				data = new Data(0, null, fieldType, currentDF);
			}
			
			switch(function) {
				case FNC_MIN:
					if(firstLoop) {
						data.setValue(currentValue);
						firstLoop = false;
					} else {
						if(currentDF.compareTo(data.getValue()) < 0) data.setValue(currentValue);
					}
					break;
				case FNC_MAX:
					if(firstLoop) {
						data.setValue(currentValue);
						firstLoop = false;
					} else {
						if(currentDF.compareTo(data.getValue()) > 0) data.setValue(currentValue);
					}
					break;
				case FNC_SUM:
					data.increaseValue(currentValue);
					break;
				case FNC_COUNT:
					data.increaseCount();
					break;
				case FNC_AVG:
					data.increaseValue(currentValue);
					data.increaseCount();
					break;
				case FNC_STDEV:
					data.increaseValue(currentValue);
					data.increaseCount();
					break;
			}
		}
		
		/**
		 * Gets result of aggregate function.
		 */
		Object getValue() {
			if(data == null) return null;

			Object resultValue;
			resultValue = data.getValue();
			
			Object result = null;
			
			switch(function) {
				case FNC_MIN:
				case FNC_MAX:
					if(firstLoop) {
						result = null;
					} else {
						result = resultValue;
					}
					break;
				case FNC_SUM:
					result = resultValue;
					break;
				case FNC_COUNT:
					result = new Integer(data.getCount());
					break;
				case FNC_AVG:
					result = new Double(data.getDoubleValue() / data.getCount());
					break;
				case FNC_STDEV:
					result = new Double(data.getDoubleValue() / data.getCount());
					break;
			}
			
			data.reset();
			firstLoop = true;
			
			return result;
		}
		
		/**
		 * Update this aggregate item with record for unsorted input data.
		 */
		void updateUnsorted(DataRecord record) {
			DataField currentDF = null;
			Object currentValue = null;
			if(function != FNC_COUNT) {
				currentDF = record.getField(fieldNo);
				currentValue = currentDF.getValue();
				if(currentValue == null) return;
			}
			
			String keyStr = recordKey.getKeyString(record);

			switch(function) {
				case FNC_MIN:
					if(!dataMap.containsKey(keyStr)) {
						dataMap.put(keyStr, new Data(0, currentValue, fieldType, currentDF));
					} else {
						Data d = (Data) dataMap.get(keyStr);
						if(currentDF.compareTo(d.getValue()) < 0) 
							d.setValue(currentValue);
					}
					break;
				case FNC_MAX:
					if(!dataMap.containsKey(keyStr)) {
						dataMap.put(keyStr, new Data(0, currentValue, fieldType, currentDF));
					} else {
						Data d = (Data) dataMap.get(keyStr);
						if(currentDF.compareTo(d.getValue()) > 0) 
							d.setValue(currentValue);
					}
					break;
				case FNC_SUM:
					if(!dataMap.containsKey(keyStr)) {
						dataMap.put(keyStr, new Data(0, currentValue, fieldType, currentDF));
					} else {
						((Data) dataMap.get(keyStr)).increaseValue(currentValue);
					}
					break;
				case FNC_COUNT:
					if(!dataMap.containsKey(keyStr)) {
						dataMap.put(keyStr, new Data(1, null, fieldType, currentDF));
					} else {
						((Data) dataMap.get(keyStr)).increaseCount();
					}
					break;
				case FNC_AVG:
					if(!dataMap.containsKey(keyStr)) {
						dataMap.put(keyStr, new Data(1, currentValue, fieldType, currentDF));
					} else {
						((Data) dataMap.get(keyStr)).increaseCount();
						((Data) dataMap.get(keyStr)).increaseValue(currentValue);
					}
					break;
				case FNC_STDEV:
					if(!dataMap.containsKey(keyStr)) {
						dataMap.put(keyStr, new Data(1, currentValue, fieldType, currentDF));
					} else {
						((Data) dataMap.get(keyStr)).increaseCount();
						((Data) dataMap.get(keyStr)).increaseValue(currentValue);
					}
					break;
			}
		}

		/**
		 * Gets result of aggregate function.
		 */
		Object getValueUnsorted(String key) {
			Object resultValue;
			Data data = (Data) dataMap.get(key);
			if(data == null) return null;
			
			resultValue = data.getValue();

			switch(function) {
				case FNC_MIN:
				case FNC_MAX:
				case FNC_SUM:
					return resultValue;
				case FNC_COUNT:
					return new Integer(data.getCount());
				case FNC_AVG:
					return new Double(data.getDoubleValue() / data.getCount());
				case FNC_STDEV:
					return new Double(data.getDoubleValue() / data.getCount());
				default:
					throw new RuntimeException("Unknown aggregate function.");
			}
		}
	}

	/**
	 * Helper data container.
	 */
	class Data {
		private char fieldType;
		private int count;
		private DataField value = null;
		private DataField value1 = null;
		
		Data(int count, Object v, char fieldType, DataField df) {
			this.fieldType = fieldType;
			this.count = count;
			if(df != null) { 
				value = df.duplicate();
				value.setNull(true);
			}
			setValue(v);
		}

		public int getCount() {
			return count;
		}

		public void increaseCount() {
			count++;
		}

		private void increaseValueEx(DataField val, Object increasor) {
			switch(fieldType) {
				case DataFieldMetadata.INTEGER_FIELD:
					if(val.isNull()) {
						setValueEx(val, increasor);
					} else {
						setValueEx(val, new Integer(((Integer) val.getValue()).intValue() + ((Integer) increasor).intValue()));
					}
					break;
				case DataFieldMetadata.NUMERIC_FIELD:
					if(val.isNull()) {
						setValueEx(val, increasor);
					} else {
						setValueEx(val, new Double(((Double) val.getValue()).doubleValue() + ((Double) increasor).doubleValue()));
					}
					break;
				case DataFieldMetadata.LONG_FIELD:
					if(val.isNull()) {
						setValueEx(val, increasor);
					} else {
						setValueEx(val, new Long(((Long) val.getValue()).longValue() + ((Long) increasor).longValue()));
					}
					break;
			}
		}

		public void increaseValue(Object increasor) {
			increaseValueEx(value, increasor);
		}

		public void increaseValue1(Object increasor) {
			increaseValueEx(value1, increasor);
		}
		
		private void setValueEx(DataField val, Object v) {
			if(val != null) {
				val.setNull(false);
				val.setValue(v);
			}
		}

		public void setValue(Object v) {
			if(value != null) {
				value.setValue(v);
			}
		}
		
		public Object getValue() {
			return value != null ? value.getValue() : null;
		}
		
		public double getDoubleValue() {
			return ((Number) value).getDouble();
		}
		
		public void reset() {
			count = 0;
			setValue(null);
		}
	}
}

