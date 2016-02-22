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

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

/**
 * @author salamonp (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12. 5. 2015
 */
public class DataRecordWithLazyLoadingTest extends CloverTestCase {
	
	DataRecordMetadata metadata;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();

		metadata = new DataRecordMetadata("record");
		metadata.addField(new DataFieldMetadata("field1", DataFieldType.STRING, ";"));
		metadata.addField(new DataFieldMetadata("field2", DataFieldType.INTEGER, ";"));
		metadata.getField(1).setDefaultValueStr("123");
		metadata.getField(1).setNullable(false);
	}
	
	public void testLazyLoading() {
		DataRecordWithLazyLoading record = DataRecordFactory.newRecordWithLazyLoading(metadata);
		assertNull(record.getField("field1").getValue());
		
		record.getField("field2").setSourceData(11);
		assertEquals(11, record.getField("field2").getValue());
		
		record.getField("field2").setSourceData(150);
		assertEquals(150, record.getField("field2").getValue());
		
		record.getField("field2").setSourceData(1000);
		record.getField("field2").setToDefaultValue();
		assertEquals(123, record.getField("field2").getValue());
	}
}
