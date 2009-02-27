/**
 * 
 */
package org.jetel.data.parser;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.jetel.util.file.FileUtils;

/**
 * @author Agata Vackova (agata.vackova@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @since Feb 18, 2009
 */
public class DataParserTest extends CloverTestCase {

	private final static String TEST_FILE_UTF8 = "data/street-names.utf8.dat";
	private final static String TEST_FILE_CP1250 = "data/street-names.cp1250.dat";
	private final static String TEST_FILE_UTF16 = "data/street-names.utf16.dat";
	private final static String TEST_FILE_ISO88591 = "data/street-names.ISO88591.dat";
	
	DataRecordMetadata metadata = new DataRecordMetadata("meta", DataRecordMetadata.DELIMITED_RECORD);
	DataRecord recordUTF8, recordUTF16, recordCp1250, recordISO88591;
	
	DataParser parserUTF8 = new DataParser("UTF-8");
	DataParser parserUTF16 = new DataParser("UTF-16");
	DataParser parserCp1250 = new DataParser("windows-1250");
	DataParser parserISO88591 = new DataParser("ISO-8859-1");
	
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		initEngine();
		metadata.setFieldDelimiter("\n");
		metadata.setRecordDelimiters("\n");
		metadata.addField(new DataFieldMetadata("Field1", DataFieldMetadata.STRING_FIELD, null));
		recordUTF8 = new DataRecord(metadata);
		recordUTF8.init();
		recordUTF16 = new DataRecord(metadata);
		recordUTF16.init();
		recordCp1250 = new DataRecord(metadata);
		recordCp1250.init();
		recordISO88591 = new DataRecord(metadata);
		recordISO88591.init();
		parserUTF8.setTrim(true);
		parserUTF16.setTrim(true);
		parserCp1250.setTrim(true);
		parserISO88591.setTrim(true);
	}

	public void testParsers() throws Exception {
		parserUTF8.init(metadata);
		parserUTF8.setDataSource(FileUtils.getInputStream(null, TEST_FILE_UTF8));
		parserUTF16.init(metadata);
		parserUTF16.setDataSource(FileUtils.getInputStream(null, TEST_FILE_UTF16));
		parserCp1250.init(metadata);
		parserCp1250.setDataSource(FileUtils.getInputStream(null, TEST_FILE_CP1250));
		parserISO88591.init(metadata);
		parserISO88591.setDataSource(FileUtils.getInputStream(null, TEST_FILE_ISO88591));
		while ((recordUTF8 = parserUTF8.getNext(recordUTF8)) != null) {
			recordUTF16 = parserUTF16.getNext(recordUTF16);
			recordCp1250 = parserCp1250.getNext(recordCp1250);
			recordISO88591 = parserISO88591.getNext(recordISO88591);
			assertEquals(recordUTF8.getField(0), recordUTF16.getField(0));
			assertEquals(recordUTF8.getField(0), recordCp1250.getField(0));
			assertEquals(recordUTF8.getField(0), recordISO88591.getField(0));
		}
		parserUTF8.close();
		parserUTF16.close();
		parserUTF16.close();
		parserUTF16.close();
	}

	public void testOddBufferSize() throws Exception {
		Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE = 15;
		testParsers();
	}
	
	public void testEvenBufferSize() throws Exception {
		Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE = 16;
		testParsers();
	}
	
	@Override
	protected void tearDown() throws Exception {
		Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE = 32768;
	}
}
