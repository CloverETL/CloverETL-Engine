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

package test.org.jetel.metadata;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

import junit.framework.TestCase;

/**
 * @author Wes Maciorowski
 * @since    March 26, 2003
 * @version 1.0
 *
 */
public class DataRecordMetadataTest extends TestCase {
	private DataRecordMetadata aFixedDataRecordMetadata;
	private DataRecordMetadata aDelimitedDataRecordMetadata;
	
	protected void setUp() { 
		aFixedDataRecordMetadata = new DataRecordMetadata("record1",DataRecordMetadata.FIXEDLEN_RECORD);
		aDelimitedDataRecordMetadata = new DataRecordMetadata("record2",DataRecordMetadata.DELIMITED_RECORD);
	}

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

		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 0",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 1",DataFieldMetadata.BYTE_FIELD,":"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 2",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 3",DataFieldMetadata.INTEGER_FIELD,(short)23));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 4",DataFieldMetadata.INTEGER_FIELD,";"));
		assertEquals(5, aDelimitedDataRecordMetadata.getNumFields());
		aDataFieldMetadata  = aDelimitedDataRecordMetadata.getField(1);
		if (aDataFieldMetadata==null) System.out.println("NULL returned!");
		assertEquals(DataFieldMetadata.BYTE_FIELD, aDataFieldMetadata.getType() );
		assertEquals(":", aDataFieldMetadata.getDelimiter() );
		assertEquals("Field 1", aDataFieldMetadata.getName());
		aDataFieldMetadata  = aDelimitedDataRecordMetadata.getField(3);
		assertEquals((short)23, aDataFieldMetadata.getSize() );
	}

	/**
	 *  Test for @link org.jetel.metadata.DataRecordMetadata.getField(String _fieldName)
	 *
	 */
	public void test_2_getField() {
		DataFieldMetadata aDataFieldMetadata = null;

		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 0",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 1",DataFieldMetadata.BYTE_FIELD,":"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 2",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 3",DataFieldMetadata.INTEGER_FIELD,(short)23));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 4",DataFieldMetadata.INTEGER_FIELD,";"));
		aDataFieldMetadata  = aDelimitedDataRecordMetadata.getField("Field 1");
		assertEquals(DataFieldMetadata.BYTE_FIELD, aDataFieldMetadata.getType() );
		assertEquals(":", aDataFieldMetadata.getDelimiter() );
		assertEquals("Field 1", aDataFieldMetadata.getName());
		aDataFieldMetadata  = aDelimitedDataRecordMetadata.getField("Field 3");
		assertEquals((short)23, aDataFieldMetadata.getSize() );
	}

	/**
	 *  Test for @link org.jetel.metadata.DataRecordMetadata.getFieldPosition(String fieldName)
	 *
	 */
	public void test_getFieldPosition() {
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 0",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 1",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 2",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 3",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 4",DataFieldMetadata.INTEGER_FIELD,";"));
		assertEquals(0, aDelimitedDataRecordMetadata.getFieldPosition("Field 0"));
		assertEquals(1, aDelimitedDataRecordMetadata.getFieldPosition("Field 1"));
		assertEquals(2, aDelimitedDataRecordMetadata.getFieldPosition("Field 2"));
		assertEquals(3, aDelimitedDataRecordMetadata.getFieldPosition("Field 3"));
		assertEquals(4, aDelimitedDataRecordMetadata.getFieldPosition("Field 4"));
	}

	/**
	 *  Test for @link org.jetel.metadata.DataRecordMetadata.addField(DataFieldMetadata _field)
	 *
	 */
	public void test_addField() {
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 0",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 1",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 2",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 3",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 4",DataFieldMetadata.INTEGER_FIELD,";"));
		assertEquals(5, aDelimitedDataRecordMetadata.getNumFields());
	}

	/**
	 *  Test for @link org.jetel.metadata.DataRecordMetadata.delField(short _fieldNum)
	 *
	 */
	public void test_1_delField() {
		DataFieldMetadata aDataFieldMetadata = null;
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 0",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 1",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 2",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 3",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 4",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.delField((short)2);
		assertEquals(4, aDelimitedDataRecordMetadata.getNumFields());
		aDataFieldMetadata  = aDelimitedDataRecordMetadata.getField(2);
		assertNotSame("Field 1", aDataFieldMetadata.getName());
	}


	/**
	 *  Test for @link org.jetel.metadata.DataRecordMetadata.delField(String _fieldName)
	 *
	 */
	public void test_2_delField() {
		DataFieldMetadata aDataFieldMetadata = null;
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 0",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 1",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 2",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 3",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 4",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.delField("Field 4");
		assertEquals(4, aDelimitedDataRecordMetadata.getNumFields());
		aDataFieldMetadata  = aDelimitedDataRecordMetadata.getField(3);
		assertNotSame("Field 3", aDataFieldMetadata.getName());
	}
}