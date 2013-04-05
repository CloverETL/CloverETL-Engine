/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Created on Mar 26, 2003
 *  Copyright (C) 2003, 2002  David Pavlis, Wes Maciorowski
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package org.jetel.metadata;

import org.jetel.exception.ConfigurationStatus;
import org.jetel.test.CloverTestCase;

/**
 * @author Wes Maciorowski
 * @since    March 26, 2003
 * @version 1.0
 *
 */
public class DataRecordMetadataTest extends CloverTestCase {
	private DataRecordMetadata aFixedDataRecordMetadata;
	private DataRecordMetadata aDelimitedDataRecordMetadata;
	private DataRecordMetadata aMixedDataRecordMetadata;
	
	@Override
	protected void setUp() throws Exception { 
		super.setUp();
		
		aFixedDataRecordMetadata = new DataRecordMetadata("record1",DataRecordMetadata.FIXEDLEN_RECORD);
		aDelimitedDataRecordMetadata = new DataRecordMetadata("record2",DataRecordMetadata.DELIMITED_RECORD);
	}

	@Override
	protected void tearDown() {
		aFixedDataRecordMetadata = null;
		aDelimitedDataRecordMetadata = null;
	}


	/**
	 *  Test for @link org.jetel.metadata.DataRecordMetadata.getField(int _fieldNum)
	 *
	 */
	public void test_1_getField() {
		DataFieldMetadata aDataFieldMetadata = null;

		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field0",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field1",DataFieldMetadata.BYTE_FIELD,":"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field2",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field3",DataFieldMetadata.INTEGER_FIELD,(short)23));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field4",DataFieldMetadata.INTEGER_FIELD,";"));
		assertEquals(5, aDelimitedDataRecordMetadata.getNumFields());
		aDataFieldMetadata  = aDelimitedDataRecordMetadata.getField(1);
		assertNotNull(aDataFieldMetadata);
		assertEquals(DataFieldMetadata.BYTE_FIELD, aDataFieldMetadata.getType() );
		assertEquals(":", aDataFieldMetadata.getDelimiters()[0] );
		assertEquals("Field1", aDataFieldMetadata.getName());
		aDataFieldMetadata  = aDelimitedDataRecordMetadata.getField(3);
		assertEquals((short)23, aDataFieldMetadata.getSize() );
	}

	/**
	 *  Test for @link org.jetel.metadata.DataRecordMetadata.getField(String _fieldName)
	 *
	 */
	public void test_2_getField() {
		DataFieldMetadata aDataFieldMetadata = null;

		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field0",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field1",DataFieldMetadata.BYTE_FIELD,":"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field2",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field3",DataFieldMetadata.INTEGER_FIELD,(short)23));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field4",DataFieldMetadata.INTEGER_FIELD,";"));
		aDataFieldMetadata  = aDelimitedDataRecordMetadata.getField("Field1");
		assertEquals(DataFieldMetadata.BYTE_FIELD, aDataFieldMetadata.getType() );
		assertEquals(":", aDataFieldMetadata.getDelimiters()[0] );
		assertEquals("Field1", aDataFieldMetadata.getName());
		aDataFieldMetadata  = aDelimitedDataRecordMetadata.getField("Field3");
		assertEquals((short)23, aDataFieldMetadata.getSize() );
	}

	/**
	 *  Test for @link org.jetel.metadata.DataRecordMetadata.getFieldPosition(String fieldName)
	 *
	 */
	public void test_getFieldPosition() {
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field0",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field1",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field2",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field3",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field4",DataFieldMetadata.INTEGER_FIELD,";"));
		assertEquals(0, aDelimitedDataRecordMetadata.getFieldPosition("Field0"));
		assertEquals(1, aDelimitedDataRecordMetadata.getFieldPosition("Field1"));
		assertEquals(2, aDelimitedDataRecordMetadata.getFieldPosition("Field2"));
		assertEquals(3, aDelimitedDataRecordMetadata.getFieldPosition("Field3"));
		assertEquals(4, aDelimitedDataRecordMetadata.getFieldPosition("Field4"));
	}

	/**
	 *  Test for @link org.jetel.metadata.DataRecordMetadata.addField(DataFieldMetadata _field)
	 *
	 */
	public void test_addField() {
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field0",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field1",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field2",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field3",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field4",DataFieldMetadata.INTEGER_FIELD,";"));
		assertEquals(5, aDelimitedDataRecordMetadata.getNumFields());
	}

	/**
	 *  Test for @link org.jetel.metadata.DataRecordMetadata.delField(short _fieldNum)
	 *
	 */
	public void test_1_delField() {
		DataFieldMetadata aDataFieldMetadata = null;
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field0",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field1",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field2",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field3",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field4",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.delField((short)2);
		assertEquals(4, aDelimitedDataRecordMetadata.getNumFields());
		aDataFieldMetadata  = aDelimitedDataRecordMetadata.getField(2);
		assertNotSame("Field1", aDataFieldMetadata.getName());
	}


	/**
	 *  Test for @link org.jetel.metadata.DataRecordMetadata.delField(String _fieldName)
	 *
	 */
	public void test_2_delField() {
		DataFieldMetadata aDataFieldMetadata = null;
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field0",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field1",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field2",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field3",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field4",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.delField("Field4");
		assertEquals(4, aDelimitedDataRecordMetadata.getNumFields());
		aDataFieldMetadata  = aDelimitedDataRecordMetadata.getField(3);
		assertEquals("Field3", aDataFieldMetadata.getName());
	}
	
	public void test_checkConfigDelimited(){
		
		ConfigurationStatus status = new ConfigurationStatus();
		aDelimitedDataRecordMetadata.checkConfig(status);
		assertEquals(1, status.size());
		status.clear();
		
		DataFieldMetadata aDataFieldMetadata = new DataFieldMetadata("Field0",DataFieldMetadata.INTEGER_FIELD,null);
		aDataFieldMetadata.setDefaultValueStr("0");
		aDelimitedDataRecordMetadata.addField(aDataFieldMetadata);
		aDataFieldMetadata = new DataFieldMetadata("Field0",DataFieldMetadata.INTEGER_FIELD,";");
		aDataFieldMetadata.setDefaultValueStr("1");
		aDelimitedDataRecordMetadata.addField(aDataFieldMetadata);
		aDelimitedDataRecordMetadata.checkConfig(status);
		assertEquals(2, status.size());
		status.clear();
		
		aDelimitedDataRecordMetadata.delField(1);
		aDataFieldMetadata = new DataFieldMetadata("Field1",DataFieldMetadata.INTEGER_FIELD,";");
		aDataFieldMetadata.setDefaultValueStr("null");
		aDelimitedDataRecordMetadata.addField(aDataFieldMetadata);
		aDelimitedDataRecordMetadata.checkConfig(status);
		assertEquals(2, status.size());
		status.clear();
		
		aDelimitedDataRecordMetadata.setFieldDelimiter(";");
		
		aDataFieldMetadata.setNullable(false);
		aDataFieldMetadata.setDefaultValue(Integer.MIN_VALUE + 1);
		aDelimitedDataRecordMetadata.checkConfig(status);
		assertEquals(0, status.size());
		status.clear();
		
		aDataFieldMetadata.setDefaultValue(-1);
		aDelimitedDataRecordMetadata.checkConfig(status);
		assertEquals(0, status.size());
	}
	
	public void test_checkConfigFixed(){
		
		ConfigurationStatus status = new ConfigurationStatus();
		aFixedDataRecordMetadata.checkConfig(status);
		assertEquals(1, status.size());
		status.clear();
		
		DataFieldMetadata aDataFieldMetadata = new DataFieldMetadata("Field0",DataFieldMetadata.INTEGER_FIELD,null);
		aDataFieldMetadata.setDefaultValueStr("0");
		aFixedDataRecordMetadata.addField(aDataFieldMetadata);
		aDataFieldMetadata = new DataFieldMetadata("Field0",DataFieldMetadata.INTEGER_FIELD,(short)4);
		aDataFieldMetadata.setDefaultValueStr("1");
		aFixedDataRecordMetadata.addField(aDataFieldMetadata);
		aFixedDataRecordMetadata.checkConfig(status);
		assertEquals(2, status.size());
		status.clear();
		
		aFixedDataRecordMetadata.delField(1);
		aDataFieldMetadata = new DataFieldMetadata("Field1",DataFieldMetadata.INTEGER_FIELD,(short)4);
		aDataFieldMetadata.setDefaultValueStr("null");
		aFixedDataRecordMetadata.addField(aDataFieldMetadata);
		aFixedDataRecordMetadata.checkConfig(status);
		assertEquals(2, status.size());
		status.clear();
		
		aFixedDataRecordMetadata.getField(0).setSize((short)2);
		
		aDataFieldMetadata.setNullable(false);
		aDataFieldMetadata.setDefaultValue(Integer.MIN_VALUE + 1);
		aFixedDataRecordMetadata.checkConfig(status);
		assertEquals(0, status.size());
		status.clear();
		
		aDataFieldMetadata.setDefaultValue(-1);
		aFixedDataRecordMetadata.checkConfig(status);
		assertEquals(0, status.size());
	}

	public void test_checkConfigMixed(){
		
		try {
			aMixedDataRecordMetadata= new DataRecordMetadata("record3",'u');
			assertTrue("Unreported error with unknown record parsing type.", false);
		} catch (IllegalArgumentException e) {
			//OK
		}

		ConfigurationStatus status = new ConfigurationStatus();

		aMixedDataRecordMetadata = new DataRecordMetadata("record3", DataRecordMetadata.MIXED_RECORD);
		
		DataFieldMetadata aDataFieldMetadata = new DataFieldMetadata("Field0",DataFieldMetadata.INTEGER_FIELD,null);
		aDataFieldMetadata.setDefaultValueStr("0");
		aMixedDataRecordMetadata.addField(aDataFieldMetadata);
		aDataFieldMetadata = new DataFieldMetadata("Field0",DataFieldMetadata.INTEGER_FIELD,";");
		aDataFieldMetadata.setDefaultValueStr("1");
		aMixedDataRecordMetadata.addField(aDataFieldMetadata);
		aMixedDataRecordMetadata.checkConfig(status);
		assertEquals(2, status.size());
		status.clear();
		
		aMixedDataRecordMetadata.delField(1);
		aDataFieldMetadata = new DataFieldMetadata("Field1",DataFieldMetadata.INTEGER_FIELD,";");
		aDataFieldMetadata.setDefaultValueStr("null");
		aMixedDataRecordMetadata.addField(aDataFieldMetadata);
		aMixedDataRecordMetadata.checkConfig(status);
		assertEquals(2, status.size());
		status.clear();
		
		aMixedDataRecordMetadata.getField(0).setSize((short)4);
		
		aDataFieldMetadata.setNullable(false);
		aDataFieldMetadata.setDefaultValue(Integer.MIN_VALUE + 1);
		aMixedDataRecordMetadata.checkConfig(status);
		assertEquals(0, status.size());
		status.clear();
		
		aDataFieldMetadata.setDefaultValue(-1);
		aMixedDataRecordMetadata.checkConfig(status);
		assertEquals(0, status.size());
	}
	
	public void test_normalize() {
		DataRecordMetadata metadata = new DataRecordMetadata("_");
		metadata.setLabel("Žluťoučký kůň");
		String[] labels = new String[] {"políčko", "políčko 1", "políčko1", "políčko2", "políčko", "políčko 1", "#políčko", "$políčko1", "!políčko_1", "políčko 4", "&políčko"};
		for (String label: labels) {
			DataFieldMetadata field = new DataFieldMetadata("_", "|");
			field.setLabel(label);
			metadata.addField(field);
		}
		
		metadata.normalize();
		
		String[] expected = new String[] {"policko", "policko_1", "policko1", "policko2", "policko_2", "policko_1_1", "policko_3", "policko1_1", "policko_1_2", "policko_4", "policko_5"};
		
		assertEquals("Zlutoucky_kun", metadata.getName());
		
		for (int i = 0; i < expected.length; i++) {
			assertEquals(expected[i], metadata.getField(i).getName());
		}
		
		assertNotNull(metadata.getField("policko"));
		assertNotNull(metadata.getFieldByLabel("políčko"));
	}

}