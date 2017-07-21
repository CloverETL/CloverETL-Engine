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
package org.jetel.metadata;

import static org.jetel.metadata.DataFieldFormatType.BINARY;
import static org.jetel.metadata.DataFieldFormatType.EXCEL;
import static org.jetel.metadata.DataFieldFormatType.JAVA;
import static org.jetel.metadata.DataFieldFormatType.JODA;

import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 13 Jan 2012
 */
public class DataFieldMetadataTest extends CloverTestCase {

	public void testGetFormatStr() {
		DataFieldMetadata fieldMetadata = new DataFieldMetadata("field", ";");
		
		fieldMetadata.setFormatStr("neco");
		assertEquals("neco", fieldMetadata.getFormatStr());

		fieldMetadata.setFormatStr("java:neco");
		assertEquals("java:neco", fieldMetadata.getFormatStr());

		fieldMetadata.setFormatStr(":neco");
		assertEquals(":neco", fieldMetadata.getFormatStr());

		fieldMetadata.setFormatStr("");
		assertEquals("", fieldMetadata.getFormatStr());

		fieldMetadata.setFormatStr(null);
		assertEquals(null, fieldMetadata.getFormatStr());
	}
	
	public void testGetFormat() {
		DataFieldMetadata fieldMetadata = new DataFieldMetadata("field", ";");
		
		fieldMetadata.setFormatStr("neco");
		assertEquals("neco", fieldMetadata.getFormat());

		fieldMetadata.setFormatStr("java:neco");
		assertEquals("neco", fieldMetadata.getFormat());

		fieldMetadata.setFormatStr("javaneco");
		assertEquals("javaneco", fieldMetadata.getFormat());

		fieldMetadata.setFormatStr(":neco");
		assertEquals(":neco", fieldMetadata.getFormat());

		fieldMetadata.setFormatStr("joda:");
		assertEquals("", fieldMetadata.getFormat());

		fieldMetadata.setFormatStr("joda:neco");
		assertEquals("", fieldMetadata.getFormat());

		fieldMetadata.setFormatStr("binary:neco");
		assertEquals("", fieldMetadata.getFormat());
	}

	public void testGetFormat2() {
		DataFieldMetadata fieldMetadata = new DataFieldMetadata("field", ";");
		
		fieldMetadata.setFormatStr("neco");
		assertEquals("neco", fieldMetadata.getFormat(JAVA));

		fieldMetadata.setFormatStr("java:neco");
		assertEquals("neco", fieldMetadata.getFormat(JAVA));

		fieldMetadata.setFormatStr("javaneco");
		assertEquals("javaneco", fieldMetadata.getFormat(JAVA));

		fieldMetadata.setFormatStr(":neco");
		assertEquals(":neco", fieldMetadata.getFormat(JAVA));

		fieldMetadata.setFormatStr("joda:");
		assertEquals("", fieldMetadata.getFormat(JAVA));

		fieldMetadata.setFormatStr("joda:neco");
		assertEquals("", fieldMetadata.getFormat(JAVA));

		fieldMetadata.setFormatStr("binary:neco");
		assertEquals("", fieldMetadata.getFormat(JAVA));
		
		fieldMetadata.setFormatStr("neco");
		assertEquals("", fieldMetadata.getFormat(JODA));

		fieldMetadata.setFormatStr("java:neco");
		assertEquals("", fieldMetadata.getFormat(BINARY));

		fieldMetadata.setFormatStr("javaneco");
		assertEquals("", fieldMetadata.getFormat(EXCEL));

		fieldMetadata.setFormatStr(":neco");
		assertEquals("", fieldMetadata.getFormat(JODA));

		fieldMetadata.setFormatStr("joda:");
		assertEquals("", fieldMetadata.getFormat(JODA));

		fieldMetadata.setFormatStr("joda:neco");
		assertEquals("neco", fieldMetadata.getFormat(JODA));

		fieldMetadata.setFormatStr("binary:neco");
		assertEquals("neco", fieldMetadata.getFormat(BINARY));
	}
	
	public void testGetFormatType() {
		DataFieldMetadata fieldMetadata = new DataFieldMetadata("field", ";");
		
		fieldMetadata.setFormatStr(null);
		assertEquals(null, fieldMetadata.getFormatType());

		fieldMetadata.setFormatStr("");
		assertEquals(null, fieldMetadata.getFormatType());

		fieldMetadata.setFormatStr("neco");
		assertEquals(JAVA, fieldMetadata.getFormatType());

		fieldMetadata.setFormatStr("java:");
		assertEquals(JAVA, fieldMetadata.getFormatType());

		fieldMetadata.setFormatStr("java:neco");
		assertEquals(JAVA, fieldMetadata.getFormatType());

		fieldMetadata.setFormatStr("joda");
		assertEquals(JAVA, fieldMetadata.getFormatType());

		fieldMetadata.setFormatStr(" joda:");
		assertEquals(JAVA, fieldMetadata.getFormatType());

		fieldMetadata.setFormatStr("joda:");
		assertEquals(JODA, fieldMetadata.getFormatType());

		fieldMetadata.setFormatStr("binary:neco");
		assertEquals(BINARY, fieldMetadata.getFormatType());

		fieldMetadata.setFormatStr("excel:excel");
		assertEquals(EXCEL, fieldMetadata.getFormatType());
	}

}
