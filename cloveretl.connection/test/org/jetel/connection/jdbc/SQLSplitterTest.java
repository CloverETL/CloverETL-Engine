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
package org.jetel.connection.jdbc;

import java.util.Arrays;

import junit.framework.TestCase;

import org.jetel.util.string.StringUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Sep 18, 2012
 */
public class SQLSplitterTest extends TestCase {
	
	private static final String[] SQL1 = {
		"-- mark original record as Inactive if it was active",
		"update ENTITYMGR.ORG_DATA",
		"set",
		"  UPDATE_DATE = SYSDATE,", 
		"  UPDATE_USER = ${INT_DEFAULT_CREATED_BY},", 
		"  REG_STATUS_ID = '5' -- inactive",
		"where", 
		"  REG_STATUS_ID = '4' -- active",
		"  and DUNS = $DUNS",
		"  and PIT_ID = (select max(pit_id) from ENTITYMGR.ORG_DATA where DUNS = $DUNS and reg_status_id >= 4)",
		"  -- inactivate active record only in case we have submitted one;", 
		"  -- this is to handle case when there are multiple DUNS_PLUS4 - first one inactivates old record; the other are not doing anything.",
		"  and exists (select max(pit_id) from ENTITYMGR.ORG_DATA where DUNS = $DUNS and reg_status_id = 3)",
		";"
	};
	
	private static final String[] SQL2 = {
		"-- this completely artificial test;;;of multichar separator ;;;",
		"THIS IS THE 1st STATEMENT;;;",
		"ANOTHER STATEMENT HERE",
		";;;",
		"/* Let's have a multiline comment ", 
		" * which is going", 
		" * to have a separator ;;; inside",
		" */",
		"SELECT /* ;;; */ ';;;', count(*) FROM TableOf3thStatement ;;;",
		"HERE COMES THE LAST (4th) STATEMENT -- followed by a comment ;;; hmmm....", 
	};
	
	private static final String join(String[] input) {
		return StringUtils.join(Arrays.asList(input), System.getProperty("line.separator"));
	}
	
	private void doTestSplit(String query, int expected) {
		doTestSplit(query, ";", expected);
	}
	
	private void doTestSplit(String query, String delimiter, int expected) {
		String[] result = SQLUtil.split(query, delimiter);
		if (";".equals(delimiter)) {
			assertTrue(Arrays.equals(result, SQLUtil.split(query, null)));
		}
		System.out.println(Arrays.toString(result));
		assertEquals(expected, result.length);
		String joined = StringUtils.join(Arrays.asList(result), delimiter);
		if (query.endsWith(delimiter)) {
			joined = joined + delimiter;
		}
		assertEquals(query, joined);
	}

	/**
	 * Test method for {@link org.jetel.connection.jdbc.SQLUtil#split(String)}.
	 */
	public void testSplit() {
		doTestSplit(join(SQL1), 1);

		doTestSplit("insert into table; delete from table; drop table", 3);
		
		doTestSplit("insert into table; delete from table; drop table;", 3);

		doTestSplit("insert into table; -- this is a comment; \r\n delete from table; drop table", 3);
		
		doTestSplit("insert into table; /* this is a comment; */ delete from table; drop table", 3);

		doTestSplit("insert into table; /* this is a comment; */ delete from table; drop table", 3);
		
		doTestSplit("insert into table_name (column_name) values ('This is a test; ignore this entry');", 1);
	}

	/**
	 * Test method for {@link org.jetel.connection.jdbc.SQLUtil#split(String, String)}.
	 */
	public void testSplitWithCustomSeparator() {
		doTestSplit(join(SQL2), ";;;", 4);
		doTestSplit(join(SQL1), "active", 1);
	}
}
