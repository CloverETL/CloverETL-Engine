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
package org.jetel.data;

import java.util.Arrays;
import java.util.Random;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.jetel.util.DataGenerator;
import org.jetel.util.key.OrderType;

/**
 * @author avackova (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27 Jan 2011
 */
public class RecordComparatorAnyOrdertypeTest extends CloverTestCase {
	
	private static final int TEST_SIZE = 100;
	DataRecord record;
	DataRecord next;
	
	RecordComapratorAnyOrderType comaparator = new RecordComapratorAnyOrderType(new int[] {0,1});
	OrderType[] orderings = new OrderType[2];
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		
		DataRecordMetadata metadata = new DataRecordMetadata("test", DataRecordMetadata.DELIMITED_RECORD);
		metadata.addField(new DataFieldMetadata("int_field", DataFieldMetadata.INTEGER_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("string_field", DataFieldMetadata.STRING_FIELD, ";"));
		record = DataRecordFactory.newRecord(metadata);
		record.init();
		next = DataRecordFactory.newRecord(metadata);
		next.init();
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	/**
	 * Test method for {@link org.jetel.data.RecordComparator#compare(java.lang.Object, java.lang.Object)}.
	 */
	public void testCompare() {
		OrderType[] types = OrderType.values();
		Integer intValue;
		Integer intValueNext;
		String stringValue;
		String stringValueNext = null;
		DataGenerator dataGenerator = new DataGenerator();
		Random r = new Random();
		int intResult;
		int stringResult;
		int recResult;
		OrderType expectedString;
		OrderType expectedInt;
		int tmp;
		
		for (int intType = 0; intType < types.length; intType++){
			orderings[0] = types[intType];
			for (int stringType = 0; stringType < types.length; stringType++) {
				orderings[1] = types[stringType];
				comaparator.setSortOrderingsAnyType(Arrays.copyOf(orderings, orderings.length));
				expectedString = null;
				expectedInt = null;
				for (int i = 0; i < TEST_SIZE; i++) {
					intValue = r.nextInt(10);
					record.getField(0).setValue(intValue);
					intValueNext = r.nextInt(10);
					next.getField(0).setValue(intValueNext);
					intResult = intValue.compareTo(intValueNext);
					stringValue = dataGenerator.nextString(1, 3);
					record.getField(1).setValue(stringValue);
					stringValueNext = dataGenerator.nextString(1, 3);
					next.getField(1).setValue(stringValueNext);
					tmp = stringValue.compareTo(stringValueNext);
					stringResult = tmp == 0 ? 0 : tmp / Math.abs(tmp);//we wan't -1, 0, 1 only
					recResult = comaparator.compare(record, next);
//					System.out.println("Comparing: " + intValue + " - " + intValueNext + ", " 
//							+ stringValue + " - " + stringValueNext + " with orderings: [" 
//							+ orderings[0] + ", " + orderings[1] + "]. Result: " + recResult);
					if (intResult == 0) {
						expectedString = check(stringResult, recResult, orderings[1], expectedString);
						continue;
					}
					switch (orderings[0]) {
					case IGNORE:
						assertEquals(-1, recResult);
						break;
					case ASCENDING:
						assertEquals(intResult, recResult);
						break;
					case DESCENDING:
						assertEquals(intResult, -recResult);
						break;
					case AUTO:
						if (expectedInt == null) {
							expectedInt = intResult < 0 ? OrderType.ASCENDING : OrderType.DESCENDING;
						}
						check(intResult, recResult, orderings[0], expectedInt);
						break;
					default:
						throw new RuntimeException("Unknown order type: " + orderings[0]);
					}
				}
			}
		}
	}

	/**
	 * @param expResult
	 * @param recResult
	 * @param orderType
	 */
	private OrderType check(int expResult, int recResult, OrderType orderType, OrderType expected) {
		if (expResult == 0 && recResult == 0) {
			return expected; //the same value - OK
		}
		switch (orderType) {
		case ASCENDING:
			assertEquals(expResult, recResult);
			break;
		case IGNORE:
			assertEquals(-1, recResult);
			break;
		case DESCENDING:
			assertEquals(-expResult, recResult);
			break;
		case AUTO:
			if (expected == null) {
				expected = expResult < 0 ? OrderType.ASCENDING : OrderType.DESCENDING;
			}
			check(expResult, recResult, expected, expected);
			break;
		default:
			throw new RuntimeException("Unknown order type: " + orderType);
		}
		return expected;
	}


}
