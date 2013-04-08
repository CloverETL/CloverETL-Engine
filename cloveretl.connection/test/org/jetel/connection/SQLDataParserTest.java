package org.jetel.connection;

import java.io.FileInputStream;

import org.jetel.connection.jdbc.DBConnectionImpl;
import org.jetel.connection.jdbc.SQLDataParser;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.database.sql.DBConnection;
import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;
import org.jetel.test.CloverTestCase;

/**
 * @author maciorowski
 * 
 */
public class SQLDataParserTest extends CloverTestCase {
	private SQLDataParser aParser2 = null;

	private DataRecord record;

	@Override
	@SuppressWarnings("deprecation")
	protected void setUp() throws Exception {
		super.setUp();
		
		DataRecordMetadata metadata = null;
		DataRecordMetadataXMLReaderWriter xmlReader = new DataRecordMetadataXMLReaderWriter();
		DBConnection aDBConnection = null;

		// metadata = xmlReader.read(new
		// FileInputStream("config\\test\\rec_def\\db_null_def_rec.xml"));
		// aDBConnection = new DBConnection("",
		// "config\\test\\msaccess.clover_test.txt");
		metadata = xmlReader.read(new FileInputStream("../cloveretl.engine/config/test/rec_def/db_def_rec.xml"));
		aDBConnection = new DBConnectionImpl("conn", "../cloveretl.connection/test/org/jetel/connection/koule_postgre.cfg");

		aDBConnection.init();

		record = DataRecordFactory.newRecord(metadata);
		record.init();

		aParser2 = new SQLDataParser(metadata,"connection", "SELECT * FROM good");

		aParser2.init();
		aParser2.setDataSource(aDBConnection.getConnection(aDBConnection.getId()));
	}

	@Override
	protected void tearDown() {
		aParser2.close();
		aParser2 = null;
		record = null;
	}

	public void test_parsing() {
		int recCount = 0;
		try {
			while ((record = aParser2.getNext(record)) != null) {
				if (recCount == 0) {
					assertEquals(record.getField(0).toString(), "1.0");
					assertEquals(record.getField(1).toString(), "Stone");
					assertEquals(record.getField(2).toString(), "101");
					assertEquals(record.getField(3).toString(), "1993-01-11 00:00:00");
				} else if (recCount == 2) {
					assertEquals(record.getField(0).toString(), "-15.5");
					assertEquals(record.getField(2).toString(), "112");
					assertEquals(record.getField(1).toString(), "Brook");
					assertEquals(record.getField(3).toString(), "2002-11-03 00:00:00");
				} else if (recCount == 3) {
					assertEquals(record.getField(0).toString(), "-0.7");
					assertEquals(record.getField(1).toString(), "Bone Broo");
					assertEquals(record.getField(2).toString(), "99");
					assertEquals(record.getField(3).toString(), "2003-01-01 00:00:00");
				}
				recCount++;
			}
		} catch (BadDataFormatException e) {
			fail("Should not raise an BadDataFormatException");
			e.printStackTrace();
		} catch (Exception ee) {
			fail("Should not throw Exception");
			ee.printStackTrace();
		}
		assertEquals(4, recCount);
	}

}
