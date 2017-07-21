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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;

/**
 * @author Raszyk (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5.8.2013
 */
public class SQLScriptParserTest extends TestCase {

	public void testSQLScriptParser() throws IOException {
		parse(1, "hello; ");
		parse(2, "hello1;\nhello2;");
		parse(3, "hello1;hello2;hello3;\n\r\n\r\t \n");
		parse(1, "hello1;-- someth;ing");
		parse(1, "hello1;-- someth;ing\n-- something\n");
		parse(1, "hello1 '/* ;; -- */;';");
		parse(3, ";;;");
		parse(1, "--garbage\nhello --comment\n world; -- garbage");
		parse(1, "copy QAHO.tt(id) from local 'tea.csv' DELIMITER ';' ENCLOSED BY '\"' DIRECT EXCEPTIONS './copy.log' REJECTED DATA './rejected_baselinetask.txt' REJECTMAX 1000;");
		parse(2, "something; \nsomethin2; \n\n");
		parse(2, "something; \nsomethin2; \n-- comment; \n\n");
		parse(2, "||a||","||");
		parse(1, "HELLO 'QUOTE \\' ;IN; \\' STRING';", ";", true);
		parse(3, "HELLO 'QUOTE \\' ;IN; \\' STRING';", ";", false);
		parse(1, "HELLO 'QUOTE '' ;IN; '' STRING';");
		parse(1, SQL1, ";");
		parse(3, "hello; /* comment 1 */; /*comment 2 */; -- terminating comment\n");
		parse(1, "/*comment*/;");
		parse(1, "/* intro comment */ hello;");
		parse(2, "/* intro comment */; hello;");
		parse(2, "hello; /* final comment */");
		parse(1, "    hello ;      \n  ");
		parse(1, "  -- foobar\n  hello ;    -- foobar\n   \n  ");
		parse(1, "/* foobar */\n/* foobar */\n/* foobar */\n");
		parse(1, "INSERT INTO TestTable VALUES ('String with separator ; inside');");
		parse(1, "insert into customer (firstname) values ('abcd;ab''cd') ;");
	}
	
	public void testOptimizationQuery() throws IOException {
		List<String> parse = parse(1, "select /*+ PARALLEL(4) */ count(*) from some_table;");
		Assert.assertEquals("select /*+ PARALLEL(4) */ count(*) from some_table", parse.get(0));
	}
	
	public void testNoStripFromStrings() throws IOException {
		String sql = "update test_varchar set\nx = ' what -- \"===;==\"\n'\nwhere 0 = 1;";
		List<String> parse = parse(1, sql);
		Assert.assertEquals("update test_varchar set\nx = ' what -- \"===;==\"\n'\nwhere 0 = 1", parse.get(0));
	}

	public void testUnexpectedEnd() {
		// assumes requireLastDelimiter to be true
		assertParseException("UNEXPECTED END'");
		assertParseException("UNEXPECTED END /*");
		assertParseException("NO DELIMITER");
	}
	
	public void testLastDelimiter() throws IOException {
		SQLScriptParser sqlScriptParser = new SQLScriptParser("NO DELIMITER", ";");
		sqlScriptParser.setRequireLastDelimiter(false);
		while (sqlScriptParser.getNextStatement() != null) { ; }

		sqlScriptParser.setStringInput("DELIMITER;");
		sqlScriptParser.setRequireLastDelimiter(true);
		while (sqlScriptParser.getNextStatement() != null) { ; }
	}
	
	public void testIterator() {
		SQLScriptParser sqlScriptParser = new SQLScriptParser("1;2;3;", ";");
		for (String s : sqlScriptParser) {
			Integer.parseInt(s);
		}
	}
	
	private static final String SQL1 =
		"-- mark original record as Inactive if it was active\n" +
		"update ENTITYMGR.ORG_DATA\n" +
		"set\n" +
		"  UPDATE_DATE = SYSDATE,\n" + 
		"  UPDATE_USER = ${INT_DEFAULT_CREATED_BY},\n" + 
		"  REG_STATUS_ID = '5' -- inactive\n" +
		"where\n" + 
		"  REG_STATUS_ID = '4' -- active\n" +
		"  and DUNS = $DUNS\n" +
		"  and PIT_ID = (select max(pit_id) from ENTITYMGR.ORG_DATA where DUNS = $DUNS and reg_status_id >= 4)\n" +
		"  -- inactivate active record only in case we have submitted one;\n" + 
		"  -- this is to handle case when there are multiple DUNS_PLUS4 - first one inactivates old record; the other are not doing anything.\n" +
		"  and exists (select max(pit_id) from ENTITYMGR.ORG_DATA where DUNS = $DUNS and reg_status_id = 3)\n" +
		";";
	
	private static List<String> parse(int expectedCount, String str) throws IOException {
		return parse(expectedCount, str, ";");
	}
	
	private static List<String> parse(int expectedCount, String str, String delimiter) throws IOException {
		return parse(expectedCount, str, delimiter, false);
	}
	
	private static List<String> parse(int expectedCount, String str, String delimiter, boolean backslashQuoteEscaping) throws IOException {
		Charset charset = Charset.defaultCharset();
		
		SQLScriptParser sqlScriptParser = new SQLScriptParser();
		sqlScriptParser.setBackslashQuoteEscaping(backslashQuoteEscaping);
		sqlScriptParser.setDelimiter(delimiter);
		
		sqlScriptParser.setStringInput(str);
		parse(expectedCount, sqlScriptParser);
		
		ByteArrayInputStream stream = new ByteArrayInputStream(str.getBytes(charset));
		sqlScriptParser.setStreamInput(stream, charset);
		parse(expectedCount, sqlScriptParser);
		
		ByteArrayInputStream streamForChannel = new ByteArrayInputStream(str.getBytes(charset));
		ReadableByteChannel channel = Channels.newChannel(streamForChannel);
		sqlScriptParser.setChannelInput(channel, charset);
		return parse(expectedCount, sqlScriptParser);
	}

	private static List<String> parse(int expectedCount, SQLScriptParser sqlScriptParser) throws IOException {
		String read;
		
		List<String> strings = new ArrayList<String>();
		while ((read = sqlScriptParser.getNextStatement()) != null) {
			strings.add(read);
		}
		
		Assert.assertEquals("Parsed wrong number of queries", expectedCount, strings.size());
		
		return strings;
	}
	
	private static void assertParseException(String sql) {
		SQLScriptParser parser = new SQLScriptParser(sql, ";");
		assertParseException(sql, parser);
	}

	private static void assertParseException(String sql, SQLScriptParser parser) {
		try {
			while (parser.getNextStatement() != null) {
				;
			}
		} catch (IOException e) {
			return;
		}
		Assert.fail("No IOException has been thrown for SQL:\n" + sql);
	}
	
}
