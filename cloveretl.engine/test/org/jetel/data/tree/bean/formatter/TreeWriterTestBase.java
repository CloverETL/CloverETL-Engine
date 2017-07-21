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
package org.jetel.data.tree.bean.formatter;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordParsingType;
import org.jetel.test.CloverTestCase;

/**
 * @author tkramolis (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 6 Mar 2012
 */
public abstract class TreeWriterTestBase extends CloverTestCase {

	protected static final String BOOLEAN_FIELD = "booleanField";
	protected static final String BYTE_FIELD = "byteField";
	protected static final String INTEGER_FIELD = "intField";
	protected static final String LONG_FIELD = "longField";
	protected static final String DECIMAL_FIELD = "decimalField";
	protected static final String DATE_FIELD = "dateField";
	protected static final String NUMERIC_FIELD = "numericField";
	protected static final String STRING_FIELD = "stringField";

	protected DataRecordMetadata metadata;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
		metadata = new DataRecordMetadata("meta", DataRecordParsingType.DELIMITED);
		metadata.setFieldDelimiter("\n");
		metadata.setRecordDelimiter("\n");
		metadata.addField(new DataFieldMetadata(BOOLEAN_FIELD, DataFieldType.BOOLEAN, "|"));
		metadata.addField(new DataFieldMetadata(BYTE_FIELD, DataFieldType.BYTE, "|"));
		metadata.addField(new DataFieldMetadata(INTEGER_FIELD, DataFieldType.INTEGER, "|"));
		metadata.addField(new DataFieldMetadata(LONG_FIELD, DataFieldType.LONG, "|"));
		metadata.addField(new DataFieldMetadata(DECIMAL_FIELD, DataFieldType.DECIMAL, "|"));
		metadata.addField(new DataFieldMetadata(DATE_FIELD, DataFieldType.DATE, "|"));
		metadata.addField(new DataFieldMetadata(NUMERIC_FIELD, DataFieldType.NUMBER, "|"));
		metadata.addField(new DataFieldMetadata(STRING_FIELD, DataFieldType.STRING, "|"));
	}

}
