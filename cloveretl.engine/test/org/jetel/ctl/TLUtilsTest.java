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
package org.jetel.ctl;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 24 Nov 2011
 */
public class TLUtilsTest extends CloverTestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
	}
	
	public void testEqualsRecordMetadata() {
		DataRecordMetadata metadata1 = new DataRecordMetadata("metadata1");
		DataRecordMetadata metadata2 = new DataRecordMetadata("metadata2");
		
		assertFalse(TLUtils.equals((DataRecordMetadata) null, null));
		assertFalse(TLUtils.equals(metadata1, null));
		assertFalse(TLUtils.equals(null, metadata1));
		assertTrue(TLUtils.equals(metadata1, metadata1));
		assertTrue(TLUtils.equals(metadata1, metadata2));
		assertTrue(TLUtils.equals(metadata2, metadata1));
		
		metadata1.addField(new DataFieldMetadata("field1", ";"));
		assertFalse(TLUtils.equals(metadata1, metadata2));

		metadata2.addField(new DataFieldMetadata("field1", (short) 10));
		assertTrue(TLUtils.equals(metadata1, metadata2));

		metadata1.addField(new DataFieldMetadata("field2", DataFieldMetadata.INTEGER_FIELD, ";"));
		metadata2.addField(new DataFieldMetadata("field2", DataFieldMetadata.LONG_FIELD, "|"));
		assertFalse(TLUtils.equals(metadata1, metadata2));

		metadata2.getField("field2").setType(DataFieldMetadata.INTEGER_FIELD);
		assertTrue(TLUtils.equals(metadata1, metadata2));

		metadata1.addField(new DataFieldMetadata("field3", DataFieldMetadata.INTEGER_FIELD, ";"));
		metadata1.addField(new DataFieldMetadata("field4", DataFieldMetadata.LONG_FIELD, "|"));
		metadata2.addField(new DataFieldMetadata("field4", DataFieldMetadata.LONG_FIELD, "|"));
		metadata2.addField(new DataFieldMetadata("field3", DataFieldMetadata.INTEGER_FIELD, ";"));
		assertFalse(TLUtils.equals(metadata1, metadata2));
		
		metadata2.delField("field4");
		metadata2.addField(new DataFieldMetadata("field4", DataFieldMetadata.LONG_FIELD, "|"));
		assertTrue(TLUtils.equals(metadata1, metadata2));
		
		metadata1.addField(new DataFieldMetadata("field5", DataFieldMetadata.STRING_FIELD, ";"));
		metadata2.addField(new DataFieldMetadata("field5a", DataFieldMetadata.STRING_FIELD, ";"));
		assertFalse(TLUtils.equals(metadata1, metadata2));
	}

	public void testEqualsFieldMetadata() {
		DataFieldMetadata dataField1 = new DataFieldMetadata("field1", (short) 5);
		DataFieldMetadata dataField2 = new DataFieldMetadata("field1", (short) 5);
		
		assertFalse(TLUtils.equals((DataFieldMetadata) null, null));
		assertFalse(TLUtils.equals(null, dataField1));
		assertFalse(TLUtils.equals(dataField1, null));
		assertTrue(TLUtils.equals(dataField1, dataField1));
		assertTrue(TLUtils.equals(dataField1, dataField2));
		assertTrue(TLUtils.equals(dataField2, dataField1));
		
		dataField1.setName("field1a");
		assertFalse(TLUtils.equals(dataField1, dataField2));

		dataField1.setName("field1");
		assertTrue(TLUtils.equals(dataField1, dataField2));
		
		dataField2.setType(DataFieldMetadata.INTEGER_FIELD);
		assertFalse(TLUtils.equals(dataField1, dataField2));

		dataField1.setType(DataFieldMetadata.DECIMAL_FIELD);
		dataField2.setType(DataFieldMetadata.DECIMAL_FIELD);
		assertTrue(TLUtils.equals(dataField1, dataField2));

		dataField1.setProperty(DataFieldMetadata.LENGTH_ATTR, "4");
		assertFalse(TLUtils.equals(dataField1, dataField2));

		dataField2.setProperty(DataFieldMetadata.LENGTH_ATTR, "4");
		assertTrue(TLUtils.equals(dataField1, dataField2));

		dataField2.setProperty(DataFieldMetadata.SCALE_ATTR, "2");
		assertFalse(TLUtils.equals(dataField1, dataField2));

		dataField1.setProperty(DataFieldMetadata.SCALE_ATTR, "2");
		assertTrue(TLUtils.equals(dataField1, dataField2));
	}

}
