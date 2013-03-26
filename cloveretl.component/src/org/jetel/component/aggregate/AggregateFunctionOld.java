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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.CRC32;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.IntegerDataField;
import org.jetel.data.RecordKey;
import org.jetel.data.primitive.Numeric;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.crypto.Base64;

/**
 *  Represent aggregate function of agregate component.
 *
 * @author      Martin Zatopek, Javlin Consulting s.r.o. (www.javlinconsulting.cz)
 * 
 * @since       June 27, 2005
 */
public class AggregateFunctionOld implements Iterator {

	final static int AGGREGATE_GROUP_INITIAL_CAPACITY = 512;		

	private final static int FNC_MIN = 0;
	private final static int FNC_MAX = 1;
	private final static int FNC_SUM = 2;
	private final static int FNC_COUNT = 3;
	private final static int FNC_AVG = 4;
    private final static int FNC_STDEV = 5;
    private final static int FNC_CRC32 = 6;
    private final static int FNC_MD5 = 7;
    private final static int FNC_FIRST = 8;
    private final static int FNC_LAST = 9;

	private final static String S_FNC_MIN = "MIN";
	private final static String S_FNC_MAX = "MAX";
	private final static String S_FNC_SUM = "SUM";
	private final static String S_FNC_COUNT = "COUNT";
	private final static String S_FNC_AVG = "AVG";
    private final static String S_FNC_STDEV = "STDEV";
    private final static String S_FNC_CRC32 = "CRC32";
    private final static String S_FNC_MD5 = "MD5";
    private final static String S_FNC_FIRST = "FIRST";
    private final static String S_FNC_LAST = "LAST";

	private final static String[] FNC_INDEX = {S_FNC_MIN, S_FNC_MAX, S_FNC_SUM, S_FNC_COUNT, S_FNC_AVG, S_FNC_STDEV, S_FNC_CRC32, S_FNC_MD5, S_FNC_FIRST, S_FNC_LAST};
	
	private DataRecordMetadata inMetadata;
	private DataRecordMetadata outMetadata;
	private AggregateItem[] aggregateItems;
	private String aggregateFunction;
	private RecordKey recordKey;
	private int aggregateGroupInitialCapacity;
	private boolean sorted;
	private DataRecord outRecord;
	private Map recordMap;
    private String charset;
	private CharsetEncoder encoder;
    
	/**
	 *Constructor for the AggregateFunction object
	 */
	public AggregateFunctionOld(String aggregateFunction, DataRecordMetadata inRecordMetadata, DataRecordMetadata outRecordMetadata, RecordKey recordKey, boolean sorted, String charset, int aggregateGroupInitialCapacity) {
		this.aggregateFunction = aggregateFunction;
		this.recordKey = recordKey;
		inMetadata = inRecordMetadata;
		outMetadata = outRecordMetadata;
		this.sorted = sorted;
		this.aggregateGroupInitialCapacity = aggregateGroupInitialCapacity;
        this.charset = charset;
	}

	/**
	 *Constructor for the AggregateFunction object
	 */
	public AggregateFunctionOld(String aggregateFunction, DataRecordMetadata inRecordMetadata, DataRecordMetadata outRecordMetadata, RecordKey recordKey, boolean sorted, String charset) {
		this(aggregateFunction, inRecordMetadata, outRecordMetadata, recordKey, sorted, charset, AGGREGATE_GROUP_INITIAL_CAPACITY);
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
		
        //init encoder
        if(charset == null) {
            encoder = Charset.forName(Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER).newEncoder();
        } else {
            encoder = Charset.forName(charset).newEncoder();
        }
        
        
		aggregateItems = new AggregateItem[functionParts.length];
		Map fieldNames = inMetadata.getFieldNamesMap();

		for (int i = 0; i < functionParts.length; i++) {
			functionPart = functionParts[i];
			
			//what a function?
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
            } else if(functionName.equalsIgnoreCase(S_FNC_CRC32)) {
                functionNumber = FNC_CRC32;
            } else if(functionName.equalsIgnoreCase(S_FNC_MD5)) {
                functionNumber = FNC_MD5;
            } else if(functionName.equalsIgnoreCase(S_FNC_FIRST)) {
                functionNumber = FNC_FIRST;
            } else if(functionName.equalsIgnoreCase(S_FNC_LAST)) {
                functionNumber = FNC_LAST;
			} else {
				throw new RuntimeException("Unknown aggregate function: " + functionName);
			}
			
			if(functionNumber == FNC_COUNT) { // parameter for COUNT aggregate function is not meanfull
			    if(!(outMetadata.getField(recordKey.getLength() + i).isNumeric()))
			        throw new RuntimeException("Incorrect output data type for aggregate function: " + FNC_INDEX[functionNumber]);
				aggregateItems[i] = new AggregateItem(functionNumber, -1);
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
            //test input metadata
			if(!(inMetadata.getField(fieldNum.intValue()).isNumeric())
					&& (functionNumber == FNC_SUM
					        || functionNumber == FNC_AVG
					        || functionNumber == FNC_STDEV)) {
                throw new RuntimeException("Incorrect input data type for " + FNC_INDEX[functionNumber] + " function: " + inMetadata.getField(fieldNum.intValue()).getTypeAsString());
			}
			if((functionNumber == FNC_CRC32 || functionNumber == FNC_MD5)
                    && inMetadata.getField(fieldNum.intValue()).getType() != DataFieldMetadata.STRING_FIELD) {
                throw new RuntimeException("Incorrect input data type for " + FNC_INDEX[functionNumber] + " function: " + inMetadata.getField(fieldNum.intValue()).getTypeAsString());
            }
            
            
			//test output metadata
			if(functionNumber == FNC_SUM 
			        || functionNumber == FNC_AVG
			        || functionNumber == FNC_STDEV)
			    if(!(outMetadata.getField(recordKey.getLength() + i).isNumeric()))
			        throw new RuntimeException("Incorrect output data type for aggregate function: " + FNC_INDEX[functionNumber]);
			if(functionNumber == FNC_MIN
			        || functionNumber == FNC_MAX
                    || functionNumber == FNC_FIRST
                    || functionNumber == FNC_LAST)
			    if(!(fieldMetadata.isNumeric())) {
			        if(fieldMetadata.getType() != outMetadata.getField(recordKey.getLength() + i).getType())
					    throw new RuntimeException("Incorrect output data type for aggregate function: " + FNC_INDEX[functionNumber]);
			    } else if(!(outMetadata.getField(recordKey.getLength() + i).isNumeric()))
				    throw new RuntimeException("Incorrect output data type for aggregate function: " + FNC_INDEX[functionNumber]);
			if(functionNumber == FNC_CRC32)
			    if(!(outMetadata.getField(recordKey.getLength() + i).getType() == DataFieldMetadata.LONG_FIELD)) {
                    throw new RuntimeException("Incorrect output data type for " + FNC_INDEX[functionNumber] + " function: " + outMetadata.getField(recordKey.getLength() + i).getTypeAsString());
                }
            if(functionNumber == FNC_MD5)
                if(!(outMetadata.getField(recordKey.getLength() + i).getType() == DataFieldMetadata.STRING_FIELD)) {
                    throw new RuntimeException("Incorrect output data type for " + FNC_INDEX[functionNumber] + " function: " + outMetadata.getField(recordKey.getLength() + i).getTypeAsString());
                }
            
			aggregateItems[i] = new AggregateItem(functionNumber, fieldNum.intValue());
		}
		
		if(!sorted) {
			recordMap = new HashMap(AGGREGATE_GROUP_INITIAL_CAPACITY);
		}
	}
	
	/**
	 * Add record for sorted data.
	 * @param record record
	 * @throws CharacterCodingException 
	 */
	public void addSortedRecord(DataRecord record) throws CharacterCodingException {
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
		Object val;
		for (int i = 0; i < aggregateItems.length; i++) {
			val = aggregateItems[i].getValue();
			if(val instanceof MyInteger) 
				((Numeric) outRecord.getField(keyFields.length + i)).setValue(((MyInteger) val).value);
            else if(val instanceof MyDouble)
                ((Numeric) outRecord.getField(keyFields.length + i)).setValue(((MyDouble) val).value);
            else if(val instanceof MyLong)
                ((Numeric) outRecord.getField(keyFields.length + i)).setValue(((MyLong) val).value);
			else if(val instanceof DataField) 
				outRecord.getField(keyFields.length + i).copyFrom((DataField) val);
			else
			    outRecord.getField(keyFields.length + i).setValue(val);
		}

		//reset aggregate items
		for (int i = 0; i < aggregateItems.length; i++) {
			aggregateItems[i].reset();
		}

		return outRecord;
	}
	
	/**
	 * Adds record to data structure for unsorted input data.
	 * @throws CharacterCodingException 
	 */
	public void addUnsortedRecord(DataRecord record) throws CharacterCodingException {
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
	@Override
	public void remove() {
		throw new RuntimeException("Call of unimplemented method.");
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return i.hasNext();
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public Object next() {
		int[] keyFields = recordKey.getKeyFields();
		
		String key = (String) i.next();
		
		//copy key to output record
		for (int i = 0; i < keyFields.length; i++) {
			outRecord.getField(i).copyFrom(((DataRecord) recordMap.get(key)).getField(keyFields[i]));
		}
		
		//copy agregation results
		Object val;
		for (int i = 0; i < aggregateItems.length; i++) {
			val = aggregateItems[i].getValueUnsorted(key);
			if(val instanceof MyInteger) 
				((Numeric) outRecord.getField(keyFields.length + i)).setValue(((MyInteger) val).value);
            else if(val instanceof MyDouble)
                ((Numeric) outRecord.getField(keyFields.length + i)).setValue(((MyDouble) val).value);
            else if(val instanceof MyLong)
                ((Numeric) outRecord.getField(keyFields.length + i)).setValue(((MyLong) val).value);
			else 
				outRecord.getField(keyFields.length + i).setValue(val);
		}
		
		return outRecord;
	}

	private class MyInteger {
		int value;
	}
    private class MyDouble {
        double value;
    }
    private class MyLong {
        long value;
    }
	/**
	 *  Represent one aggregate function.
	 *
	 * @author      Martin Zatopek, Javlin Consulting. (www.javlinconsulting.cz)
	 * @since
	 */
	private class AggregateItem {
		boolean firstLoop;
		int function;
		int fieldNo;
		Data data;
		Map dataMap; 
		MyInteger myInteger;
        MyDouble myDouble;
        MyLong myLong;
		CRC32 crc32 = new CRC32();
        MessageDigest md5;
        CloverBuffer dataBuffer = CloverBuffer.allocateDirect(Defaults.Record.FIELD_INITIAL_SIZE, Defaults.Record.FIELD_LIMIT_SIZE);
        
		/**
		 *Constructor for the FilterItem object
		 *
		 * @param  function   Description of the Parameter
		 * @param  fieldNo    Description of the Parameter
		 */
		AggregateItem(int function, int fieldNo) {
			this.function = function;
			this.fieldNo = fieldNo;
			
			firstLoop = true;
			
			if(!sorted) {
				dataMap = new HashMap(aggregateGroupInitialCapacity);
				
				if (dataMap == null) {
					throw new RuntimeException("Can't allocate HashSet of size: " + aggregateGroupInitialCapacity);
				}
			}

			myInteger = new MyInteger();
            myDouble = new MyDouble();
            myLong = new MyLong();
            
            try {
                md5 = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("Unexpected exception: MD5 algorithm is not available.");
            }
		}

		/**
		 * Update this aggregate item with record.
		 * @param record record
		 * @throws CharacterCodingException 
		 */
		void update(DataRecord record) throws CharacterCodingException {
			DataField currentDF = null;
			Object currentValue = null;
			if(function != FNC_COUNT) {
				currentDF = record.getField(fieldNo);
				currentValue = currentDF.getValue();
				if(currentValue == null) return;
 			}
			if(data == null) {
				data = new Data(0, null, currentDF);
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
					data.increaseValue(currentDF);
					break;
				case FNC_COUNT:
					data.increaseCount();
					break;
				case FNC_AVG:
					data.increaseValue(currentDF);
					data.increaseCount();
					break;
				case FNC_STDEV:
					data.increaseCount();
					if(firstLoop) {
						data.dValue1 = ((Numeric) currentDF).getDouble();
						data.dValue2 = 0;
						firstLoop = false;
					} else {
						double tempD = data.dValue1;
						data.dValue1 += (((Numeric) currentDF).getDouble() - data.dValue1) / data.count;
						data.dValue2 += (((Numeric) currentDF).getDouble() - tempD) * (((Numeric) currentDF).getDouble() - data.dValue1); 	
					}
					break;
                case FNC_CRC32:
                    if(firstLoop) {
                        crc32.reset();
                        firstLoop = false;
                    }
                    dataBuffer.clear();
                    currentDF.toByteBuffer(dataBuffer, encoder);
                    crc32.update(dataBuffer.array());
                    break;
                case FNC_MD5:
                    if(firstLoop) {
                        md5.reset();
                        firstLoop = false;
                    }
                    dataBuffer.clear();
                    currentDF.toByteBuffer(dataBuffer, encoder);
                    md5.update(dataBuffer.array());
                    break;
                case FNC_FIRST:
                    if(firstLoop) {
                        data.setValue(currentValue);
                        firstLoop = false;
                    }
                    break;
                case FNC_LAST:
                    data.setValue(currentValue);
                    firstLoop = false;
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
                case FNC_FIRST:
                case FNC_LAST:
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
					myInteger.value = data.count;
					result = myInteger;
					break;
				case FNC_AVG:
					myDouble.value = data.getDoubleValue() / data.count;
					result = myDouble;
					break;
				case FNC_STDEV:
					if(data.count > 1) {
						myDouble.value = Math.sqrt(data.dValue2 / (data.count - 1));
						result = myDouble;
					} else 
						result = null;
					break;
                case FNC_CRC32:
                    if(firstLoop) {
                        result = null;
                    } else {
                        myLong.value = crc32.getValue();
                        result = myLong;
                    }
                    break;
                case FNC_MD5:
                    if(firstLoop) {
                        result = null;
                    } else {
                        result = Base64.encodeBytes(md5.digest());
                    }
                    break;
			}
			
			return result;
		}
		
		void reset() {
            if(data != null) {
                data.reset();
            }
			firstLoop = true;
		}
		
		/**
		 * Update this aggregate item with record for unsorted input data.
		 * @throws CharacterCodingException 
		 */
		void updateUnsorted(DataRecord record) throws CharacterCodingException {
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
						dataMap.put(keyStr, new Data(0, currentValue, currentDF));
					} else {
						Data d = (Data) dataMap.get(keyStr);
						if(currentDF.compareTo(d.getValue()) < 0) 
							d.setValue(currentValue);
					}
					break;
				case FNC_MAX:
					if(!dataMap.containsKey(keyStr)) {
						dataMap.put(keyStr, new Data(0, currentValue, currentDF));
					} else {
						Data d = (Data) dataMap.get(keyStr);
						if(currentDF.compareTo(d.getValue()) > 0) 
							d.setValue(currentValue);
					}
					break;
				case FNC_SUM:
					if(!dataMap.containsKey(keyStr)) {
						dataMap.put(keyStr, new Data(0, currentValue, currentDF));
					} else {
						((Data) dataMap.get(keyStr)).increaseValue(currentDF);
					}
					break;
				case FNC_COUNT:
					if(!dataMap.containsKey(keyStr)) {
						dataMap.put(keyStr, new Data(1, null, currentDF));
					} else {
						((Data) dataMap.get(keyStr)).increaseCount();
					}
					break;
				case FNC_AVG:
					if(!dataMap.containsKey(keyStr)) {
						dataMap.put(keyStr, new Data(1, currentValue, currentDF));
					} else {
						((Data) dataMap.get(keyStr)).increaseCount();
						((Data) dataMap.get(keyStr)).increaseValue(currentDF);
					}
					break;
				case FNC_STDEV:
					if(!dataMap.containsKey(keyStr)) {
						dataMap.put(keyStr, new Data(1, ((Numeric) currentDF).getDouble(), 0));
					} else {
						Data tempData = (Data) dataMap.get(keyStr);
						double tempD = tempData.dValue1;
						tempData.increaseCount();
						tempData.dValue1 += (((Numeric) currentDF).getDouble() - tempData.dValue1) / tempData.count;
						tempData.dValue2 += (((Numeric) currentDF).getDouble() - tempD) * (((Numeric) currentDF).getDouble() - tempData.dValue1); 	
					}
					break;
                case FNC_CRC32:
                    if(!dataMap.containsKey(keyStr)) {
                        CRC32 crc32 = new CRC32();
                        dataBuffer.clear();
                        currentDF.toByteBuffer(dataBuffer, encoder);
                        crc32.update(dataBuffer.array());
                        dataMap.put(keyStr, crc32);
                    } else {
                        CRC32 crc32 = (CRC32) dataMap.get(keyStr);
                        dataBuffer.clear();
                        currentDF.toByteBuffer(dataBuffer, encoder);
                        crc32.update(dataBuffer.array());
                    }
                    break;
                case FNC_MD5:
                    if(!dataMap.containsKey(keyStr)) {
                        MessageDigest md5;
                        try {
                            md5 = MessageDigest.getInstance("md5");
                        } catch (NoSuchAlgorithmException e) {
                            throw new RuntimeException("Unexpected exception: MD5 algorithm is not available.");
                        }
                        dataBuffer.clear();
                        currentDF.toByteBuffer(dataBuffer, encoder);
                        md5.update(dataBuffer.array());
                        dataMap.put(keyStr, md5);
                    } else {
                        MessageDigest md5 = (MessageDigest) dataMap.get(keyStr);
                        dataBuffer.clear();
                        currentDF.toByteBuffer(dataBuffer, encoder);
                        md5.update(dataBuffer.array());
                    }
                    break;
                case FNC_FIRST:
                    if(!dataMap.containsKey(keyStr)) {
                        dataMap.put(keyStr, new Data(0, currentValue, currentDF));
                    }
                    break;
                case FNC_LAST:
                    if(!dataMap.containsKey(keyStr)) {
                        dataMap.put(keyStr, new Data(0, currentValue, currentDF));
                    } else {
                        Data d = (Data) dataMap.get(keyStr);
                        d.setValue(currentValue);
                    }
                    break;

			}
		}

		/**
		 * Gets result of aggregate function.
		 */
		Object getValueUnsorted(String key) {
			Object resultValue = null;
			
            Object oData = dataMap.get(key);
            if(oData == null) return null;
            
            Data data = null;
            if(oData instanceof Data) {
                data = (Data) oData;
			
                resultValue = data.getValue();
            }

			switch(function) {
				case FNC_MIN:
				case FNC_MAX:
				case FNC_SUM:
                case FNC_FIRST:
                case FNC_LAST:
					return resultValue;
				case FNC_COUNT:
					myInteger.value = data.count;
					return myInteger;
				case FNC_AVG:
					myDouble.value = data.getDoubleValue() / data.count;
					return myDouble;
				case FNC_STDEV:
					if(data.count > 1) {
						myDouble.value = Math.sqrt(data.dValue2 / (data.count - 1));
						return myDouble;
					} else 
						return null;
                case FNC_CRC32:
                    myLong.value = ((CRC32) oData).getValue();
                    return myLong;
                case FNC_MD5:
                    return Base64.encodeBytes(((MessageDigest) oData).digest());
				default:
					throw new RuntimeException("Unknown aggregate function.");
			}
		}
	}

	/**
	 * Helper data container.
	 */
	class Data {
		int count;
		private DataField value = null;
		double dValue1;
		double dValue2;
		
		Data(int count, Object v, DataField df) {
			this.count = count;
			if(df != null) { 
				value = df.duplicate();
				value.setNull(true);
			}
			setValue(v);
		}

		Data(int count, double dVal1, double dVal2) {
			this.count = count;
			dValue1 = dVal1;
			dValue2 = dVal2;
		}

		public void increaseCount() {
			count++;
		}

		private void increaseValueEx(DataField val, DataField increasor) {
			Numeric num = (Numeric) val;
			
			if(val.isNull()) {
				setValueEx(val, increasor);
			} else {
				num.add(((Numeric) increasor));
			}
		}

		public void increaseValue(DataField increasor) {
			increaseValueEx(value, increasor);
		}

		private void setValueEx(DataField val, DataField v) {
			if(val != null) {
				//val.setNull(false); //TODO kokon
				val.setValue(v.getValue());
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
		
		public Object getAvg() {
		    ((Numeric) value).div(new IntegerDataField(null, count));
		    return value;
		}
		
		public double getDoubleValue() {
			return ((Numeric) value).getDouble();
		}
		
		public void reset() {
			count = 0;
			if(value != null) value.setNull(true);
			dValue1 = 0;
			dValue2 = 0;
		}
	}
}

