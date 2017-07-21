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
package org.jetel.lookup;

import java.util.ConcurrentModificationException;

import junit.framework.Assert;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.lookup.LookupTable;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

/**
 * @author Raszyk (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 11.7.2013
 */
public class ClearTest extends CloverTestCase {

	
	public void testSLTClear() throws ComponentNotReadyException {
		DataRecordMetadata metadata = createMetadata("testMetadata", "intKey", "strPayload");
		String[] keys = { "intKey" };
		LookupTable slt = new SimpleLookupTable("testLookup", metadata, keys, null);
		((SimpleLookupTable) slt).setKeyDuplicates(true);
		slt.init();
		
		DataRecord dr = DataRecordFactory.newRecord(metadata);
		dr.init();
		slt.put(setRecord(dr, 1, "1"));
		slt.put(setRecord(dr, 2, "2"));
		slt.put(setRecord(dr, 2, "2.5"));
		slt.put(setRecord(dr, 2, "2.6"));
		slt.put(setRecord(dr, 3, "3"));
		
		Lookup lookup1 = slt.createLookup(new RecordKey(keys, metadata));
		
		lookup1.seek(setRecord(dr, 2, "2"));
		Assert.assertEquals(3, lookup1.getNumFound());
		Assert.assertTrue(lookup1.hasNext());
		
		dr = lookup1.next();
		Assert.assertEquals("2", dr.getField(1).getValue().toString());
		Assert.assertTrue(lookup1.hasNext());
		
		dr = lookup1.next();
		Assert.assertEquals("2.6", dr.getField(1).getValue().toString());
		Assert.assertTrue(lookup1.hasNext());
		
		slt.clear();
		
		Assert.assertTrue(lookup1.hasNext());
		dr = lookup1.next();
		Assert.assertEquals("2.5", dr.getField(1).getValue().toString());
		
		Assert.assertTrue(!lookup1.hasNext());
		
		lookup1.seek(setRecord(dr, 2, "2"));
		Assert.assertEquals(0, lookup1.getNumFound());
	}
	
	public void testRLTClear() throws ComponentNotReadyException {
		DataRecordMetadata lookupMetadata = createMetadata("testMetadata", "intFrom", "intTo", "strPayload");
		String[] startKeys = { "intFrom" };
		String[] endKeys = { "intTo" }; 
		
		LookupTable rlt = new RangeLookupTable("testRangeLookup", lookupMetadata, startKeys, endKeys, null);
		rlt.init();
		
		DataRecord ldr = DataRecordFactory.newRecord(lookupMetadata);
		ldr.init();
		rlt.put(setRecord(ldr, 10, 20, "10-20"));
		rlt.put(setRecord(ldr, 12, 22, "12-22"));
		rlt.put(setRecord(ldr, 30, 50, "30-50"));
		
		DataRecordMetadata inputMetadata = createMetadata("testInputMetadata", "intKeyValue", "payload");
		Lookup lookup1 = rlt.createLookup(new RecordKey(new String[] { "intKeyValue" }, inputMetadata));
		DataRecord idr = DataRecordFactory.newRecord(inputMetadata);
		idr.init();
		lookup1.seek(setRecord(idr, 15, "2"));
		
		Assert.assertEquals(2, lookup1.getNumFound());
		Assert.assertTrue(lookup1.hasNext());
		rlt.clear();
		
		try {
			ldr = lookup1.next();
			Assert.fail("ConcurrectModificationException has not been thrown");
		} catch (ConcurrentModificationException e) {
			// good
		}
		
		Lookup lookup2 = rlt.createLookup(new RecordKey(new String[] { "intKeyValue" }, inputMetadata));
		lookup2.seek(setRecord(idr, 15, "2"));
		Assert.assertEquals(0, lookup2.getNumFound());
	}
	
	// utility function
	private static DataRecordMetadata createMetadata(String metadataId, String... fields) {
		DataRecordMetadata metadata = new DataRecordMetadata(metadataId);
		for (String field : fields) {
			DataFieldMetadata metadataField;
			if (field.startsWith("int")) {
				metadataField = new DataFieldMetadata(field, DataFieldType.INTEGER, "|");
			}
			else if (field.startsWith("dec")) {
				metadataField = new DataFieldMetadata(field, DataFieldType.DECIMAL, "|");
			}
			else {
				metadataField = new DataFieldMetadata(field, DataFieldType.STRING, "|");
			}
			metadata.addField(metadataField);
		}
		
		return metadata;
	}
	
	// utility function
	private static DataRecord setRecord(DataRecord record, Object... values) {
		for (int i = 0; i < values.length; i++) {
			record.getField(i).setValue(values[i]);
		}
		return record;
	}
	
}
