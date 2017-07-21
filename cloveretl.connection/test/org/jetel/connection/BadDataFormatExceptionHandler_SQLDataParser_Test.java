package org.jetel.connection;

import java.io.FileInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.connection.jdbc.DBConnectionImpl;
import org.jetel.connection.jdbc.SQLDataParser;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.database.sql.DBConnection;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.ParserExceptionHandlerFactory;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;
import org.jetel.test.CloverTestCase;

/**
 * @author maciorowski
 * 
 */
public class BadDataFormatExceptionHandler_SQLDataParser_Test extends CloverTestCase {
	private SQLDataParser aParser1 = null;
	private SQLDataParser aParser2 = null;
	private DataRecord record;
	private DataRecordMetadata metadata = null;
	
	private static Log logger = LogFactory.getLog(BadDataFormatExceptionHandler_SQLDataParser_Test.class);


	@Override
	@SuppressWarnings("deprecation")
	protected void setUp() throws Exception {
		super.setUp();
		
		DBConnection aDBConnection = null;
		DataRecordMetadataXMLReaderWriter xmlReader = new DataRecordMetadataXMLReaderWriter();

		// metadata = xmlReader.read(new FileInputStream("config\\test\\rec_def\\db_def_rec.xml"));
		// aDBConnection = new DBConnection("", "config\\test\\msaccess.clover_test.txt");
		metadata = xmlReader.read(new FileInputStream("../cloveretl.engine/config/test/rec_def/db_def_rec.xml"));
		aDBConnection = new DBConnectionImpl("conn", "../cloveretl.connection/test/org/jetel/connection/koule_postgre.cfg");

		aDBConnection.init();

		aParser2 = new SQLDataParser(metadata, "connection", "SELECT * FROM bad");

		aParser1 = new SQLDataParser(metadata, "connection", "SELECT * FROM good");

		aParser1.init();
		aParser1.setDataSource(aDBConnection.getConnection(aDBConnection.getId()));
		aParser2.init();
		aParser2.setDataSource(aDBConnection.getConnection(aDBConnection.getId()));

		record = DataRecordFactory.newRecord(metadata);
		record.init();
	}

	@Override
	protected void tearDown() {
		aParser1.close();
		aParser1 = null;

		aParser2.close();
		aParser2 = null;
		record = null;

		metadata = null;
	}

	/**
	 * Test for
	 * 
	 * @link for a well formatted data source. No handler
	 */

	public void test_goodFile() {

		// test no handler ------------------------------------
		try {
			while ((record = aParser1.getNext(record)) != null) {
			}
		} catch (BadDataFormatException e) {
			fail("Should not raise an BadDataFormatException");
			e.printStackTrace();
		} catch (Exception ee) {
			fail("Should not throw Exception");
			ee.printStackTrace();
		}

	}

	/**
	 * Test for
	 * 
	 * @link for a well formatted data source. strict handler
	 */

	public void test_strict_goodFile() {
		IParserExceptionHandler aHandler = null;
		// test strict handler ------------------------------------
		aHandler = ParserExceptionHandlerFactory.getHandler(PolicyType.STRICT);
		aParser1.setExceptionHandler(aHandler);
		try {
			while ((record = aParser1.getNext(record)) != null) {
			}
		} catch (BadDataFormatException e) {
			fail("Should not raise an BadDataFormatException");
			e.printStackTrace();
		} catch (Exception ee) {
			fail("Should not throw Exception");
			ee.printStackTrace();
		}

	}

	/**
	 * Test for
	 * 
	 * @link for a well formatted data source. controlled handler
	 */

	public void test_controlled_goodFile() {
		IParserExceptionHandler aHandler = null;
		// test controlled handler ------------------------------------
		aHandler = ParserExceptionHandlerFactory.getHandler(PolicyType.CONTROLLED);
		aParser1.setExceptionHandler(aHandler);

		int recCount = 0;
		try {
			while ((record = aParser1.getNext(record)) != null) {
				if (recCount == 0 || recCount == 1) {
					assertEquals(record.getField(0).toString(), "1.0");
					assertEquals(record.getField(1).toString(), "Stone");
					assertEquals(record.getField(2).toString(), "101");
					assertEquals(record.getField(3).toString(), "1993-01-11 00:00:00");
				} else if (recCount == 2) {
					assertEquals(record.getField(0).toString(), "-15.5");
					assertEquals(record.getField(1).toString(), "Brook");
					assertEquals(record.getField(2).toString(), "112");
					assertEquals(record.getField(3).toString(), "2002-11-03 00:00:00");
				} else if (recCount == 3) {
					assertEquals(record.getField(0).toString(), "-0.7");
					assertEquals(record.getField(1).toString(), "Bone Broo");
					assertEquals(record.getField(2).toString(), "99");
					assertEquals(record.getField(3).toString(), "2003-01-01 00:00:00");
				}
				recCount++;
			}
			assertEquals(4, recCount);

		} catch (BadDataFormatException e) {
			fail("Should not raise an BadDataFormatException");
			e.printStackTrace();
		} catch (Exception ee) {
			fail("Should not throw Exception");
			ee.printStackTrace();
		}

	}

	/**
	 * Test for
	 * 
	 * @link for a well formatted data source. lenient handler
	 */

	public void test_lenient_goodFile() {
		IParserExceptionHandler aHandler = null;
		// test lenient handler ------------------------------------
		aHandler = ParserExceptionHandlerFactory.getHandler(PolicyType.LENIENT);
		aParser1.setExceptionHandler(aHandler);

		int recCount = 0;
		try {
			while ((record = aParser1.getNext(record)) != null) {
				if (recCount == 0 || recCount == 1) {
					assertEquals(record.getField(0).toString(), "1.0");
					assertEquals(record.getField(1).toString(), "Stone");
					assertEquals(record.getField(2).toString(), "101");
					assertEquals(record.getField(3).toString(), "1993-01-11 00:00:00");
				} else if (recCount == 2) {
					assertEquals(record.getField(0).toString(), "-15.5");
					assertEquals(record.getField(1).toString(), "Brook");
					assertEquals(record.getField(2).toString(), "112");
					assertEquals(record.getField(3).toString(), "2002-11-03 00:00:00");
				} else if (recCount == 2) {
					assertEquals(record.getField(0).toString(), "-0.7");
					assertEquals(record.getField(1).toString(), "Bone Broo");
					assertEquals(record.getField(2).toString(), "99");
					assertEquals(record.getField(3).toString(), "2003-01-01 00:00:00");
				}
				recCount++;
			}
			assertEquals(4, recCount);
		} catch (BadDataFormatException e) {
			fail("Should not raise an BadDataFormatException");
			e.printStackTrace();
		} catch (Exception ee) {
			fail("Should not throw Exception");
			ee.printStackTrace();
		}
	}

	/**
	 * Test for a data source with poorly formatted fields. No handler.
	 */

	public void test_badFile() {
		boolean failed = false;
		// test no handler ------------------------------------
		try {
			while ((record = aParser2.getNext(record)) != null) {
				fail("Should throw Exception");
			}
		} catch (BadDataFormatException e) {
			failed = true;
			System.out.println(e.getMessage());
		} catch (RuntimeException re) {
			fail("Should throw RuntimeException");
		} catch (Exception ee) {
			ee.printStackTrace();
		}
		if (!failed)
			fail("Should raise an BadDataFormatException");
	}

	/**
	 * Test for a data source with poorly formatted fields. No handler.
	 */

	public void test_strict_badFile() {
		IParserExceptionHandler aHandler = null;

		// test strict handler ------------------------------------
		aHandler = ParserExceptionHandlerFactory.getHandler(PolicyType.STRICT);
		aParser2.setExceptionHandler(aHandler);
		int recCount = 0;
		try {
			while ((record = aParser2.getNext(record)) != null) {
				recCount++;
			}
			fail("Should raise an BadDataFormatException");
		} catch (BadDataFormatException e) {
		} catch (Exception ee) {
			logger.error("Should not throw Exception",ee);
			fail("Should not throw Exception" );
		}
		assertEquals(0, recCount);
	}

	/**
	 * Test for a data source with poorly formatted fields. controlled handler.
	 */

	public void test_controlled_badFile() {
		IParserExceptionHandler aHandler = null;

		// test controlled handler ------------------------------------
		aHandler = ParserExceptionHandlerFactory.getHandler(PolicyType.CONTROLLED);
		aParser2.setExceptionHandler(aHandler);
		int recCount = 0;
		try {
			while ((record = aParser2.getNext(record)) != null) {
				recCount++;
			}
		} catch (BadDataFormatException e) {
			System.out.println(e.getMessage());
		} catch (Exception ee) {
			fail("Should not throw Exception");
			ee.printStackTrace();
		}
		assertEquals(0, recCount); // may need to be revised
		// depending how we implement nullable property

	}

	/**
	 * Test for a data source with poorly formatted fields. lenient handler.
	 */

	public void test_lenient_badFile() {
		IParserExceptionHandler aHandler = null;
		// test lenient handler ------------------------------------
		aHandler = ParserExceptionHandlerFactory.getHandler(PolicyType.LENIENT);
		aParser2.setExceptionHandler(aHandler);
		int recCount = 0;

		try {
			while ((record = aParser2.getNext(record)) != null) {
				if (recCount == 0) {
					assertEquals("No Name", record.getField(1).toString());
					assertEquals("101", record.getField(2).toString());
				} else if (recCount == 1) {
					assertEquals("Brook", record.getField(1).toString());
					assertEquals("2000-01-01 00:00:00", record.getField(3).toString());
				} else if (recCount == 2) {
					assertEquals("0.0", record.getField(0).toString());
					assertEquals("5", record.getField(2).toString());
				}
				recCount++;
			}
			assertEquals(0, recCount);
		} catch (BadDataFormatException e) {
			fail("Should not raise an BadDataFormatException");
			e.printStackTrace();
		} catch (Exception ee) {
			ee.printStackTrace();
			fail("Should not throw Exception");
		}
	}

}
