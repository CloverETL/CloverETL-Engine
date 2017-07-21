/**
 * 
 */
package org.jetel.data.parser;

import java.util.LinkedList;
import java.util.List;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.file.FileUtils;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * @author Agata Vackova (agata.vackova@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @since Feb 18, 2009
 */
public class DataParserTest extends AbstractParserTestCase {

	private final static String TEST_FILE_UTF8 = "data/street-names.utf8.dat";
	private final static String TEST_FILE_CP1250 = "data/street-names.cp1250.dat";
	private final static String TEST_FILE_UTF16 = "data/street-names.utf16.dat";
	private final static String TEST_FILE_ISO88591 = "data/street-names.ISO88591.dat";

	private final static String TEST_FILE_DELIM1 = "data/delimiter_test.dat";
	private final static String TEST_FILE_DELIM2 = "data/delimiter_test2.dat";
	private final static String TEST_FILE_DELIM3 = "data/delimiter_test3.dat";
	private final static String TEST_FILE_DELIM3_FIELDS = "data/delimiter_test3_field.dat";
	
	
	// DataRecordMetadata metadata = new DataRecordMetadata("meta", DataRecordMetadata.DELIMITED_RECORD); This doesn't work -- engine is not yet initialized
	DataRecordMetadata metadata;
	DataRecord recordUTF8, recordUTF16, recordCp1250, recordISO88591;
	
	DataParser parserUTF8;
	DataParser parserUTF16;
	DataParser parserCp1250;
	DataParser parserISO88591;
	
	private int oldBufferSize;
	
	@Override
	protected void setUp() throws Exception {
		initEngine();
		metadata = new DataRecordMetadata("meta", DataRecordMetadata.DELIMITED_RECORD);
		metadata.setFieldDelimiter("\n");
		metadata.setRecordDelimiter("\n");
		metadata.addField(new DataFieldMetadata("Field1", DataFieldMetadata.STRING_FIELD, null));

		recordUTF8 = DataRecordFactory.newRecord(metadata);
		recordUTF8.init();
		TextParserConfiguration parserUTF8Cfg = new TextParserConfiguration();
		parserUTF8Cfg.setMetadata(metadata);
		parserUTF8Cfg.setCharset("UTF-8");
		parserUTF8Cfg.setTrim(true);
		parserUTF8 = new DataParser(parserUTF8Cfg);
		
		recordUTF16 = DataRecordFactory.newRecord(metadata);
		recordUTF16.init();
		TextParserConfiguration parserUTF16Cfg = new TextParserConfiguration();
		parserUTF16Cfg.setMetadata(metadata);
		parserUTF16Cfg.setCharset("UTF-16");
		parserUTF16Cfg.setTrim(true);
		parserUTF16 = new DataParser(parserUTF16Cfg);

		
		recordCp1250 = DataRecordFactory.newRecord(metadata);
		recordCp1250.init();
		TextParserConfiguration parserCp1250Cfg = new TextParserConfiguration();
		parserCp1250Cfg.setMetadata(metadata);
		parserCp1250Cfg.setCharset("windows-1250");
		parserCp1250Cfg.setTrim(true);
		parserCp1250 = new DataParser(parserCp1250Cfg);
		
		recordISO88591 = DataRecordFactory.newRecord(metadata);
		recordISO88591.init();
		TextParserConfiguration parserISO88591Cfg = new TextParserConfiguration();
		parserISO88591Cfg.setMetadata(metadata);
		parserISO88591Cfg.setCharset("ISO-8859-1");
		parserISO88591Cfg.setTrim(true);
		parserISO88591 = new DataParser(parserISO88591Cfg);
		
		oldBufferSize = Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE;

		super.setUp();
	}

	public void testParsers() throws Exception {
		parserUTF8.init();
		parserUTF8.setDataSource(FileUtils.getInputStream(null, TEST_FILE_UTF8));
		parserUTF16.init();
		parserUTF16.setDataSource(FileUtils.getInputStream(null, TEST_FILE_UTF16));
		parserCp1250.init();
		parserCp1250.setDataSource(FileUtils.getInputStream(null, TEST_FILE_CP1250));
		parserISO88591.init();
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
	
	public void testDelimiters_genericEOL() throws Exception {
		DataRecordMetadata testMetadata = new DataRecordMetadata("meta", DataRecordMetadata.DELIMITED_RECORD);
		testMetadata.setFieldDelimiter("|");
		testMetadata.setRecordDelimiter("\n\\|\r\\|\n\r\\|\r\n");
		testMetadata.addField(new DataFieldMetadata("Field1", DataFieldMetadata.STRING_FIELD, null));
	
		DataRecord record = DataRecordFactory.newRecord(metadata);
		record.init();
		TextParserConfiguration parserConf = new TextParserConfiguration();
		parserConf.setMetadata(testMetadata);
		parserConf.setCharset("UTF-8");
		parserConf.setTrim(false);
		parserConf.setTryToMatchLongerDelimiter(true);

		
		DataParser parser = new DataParser(parserConf);
		parser.init();
		parser.setDataSource(FileUtils.getInputStream(null, TEST_FILE_DELIM1));
		
		int i=1;
		while ((record = parser.getNext(record)) != null) {
			assertEquals("text"+i, record.getField(0).getValue().toString());
			i++;
		}
		parser.close();
		
	}

	public void testDelimiters_substringContinued() throws Exception {
		DataRecordMetadata testMetadata = new DataRecordMetadata("meta", DataRecordMetadata.DELIMITED_RECORD);
		testMetadata.setFieldDelimiter("|");
		testMetadata.setRecordDelimiter("#\\|##&\\|##&&");
		testMetadata.addField(new DataFieldMetadata("Field1", DataFieldMetadata.STRING_FIELD, null));
	
		DataRecord record = DataRecordFactory.newRecord(metadata);
		record.init();
		TextParserConfiguration parserConf = new TextParserConfiguration();
		parserConf.setMetadata(testMetadata);
		parserConf.setCharset("UTF-8");
		parserConf.setTrim(false);
		parserConf.setTryToMatchLongerDelimiter(true);

		
		DataParser parser = new DataParser(parserConf);
		parser.init();
		parser.setDataSource(FileUtils.getInputStream(null, TEST_FILE_DELIM2));
		
		int i=1;
		while ((record = parser.getNext(record)) != null) {
			assertEquals("text"+i, record.getField(0).getValue().toString());
			i++;
		}
		parser.close();
		
	}
	
	public void testDelimiters_substring() throws Exception {
		DataRecordMetadata testMetadata = new DataRecordMetadata("meta", DataRecordMetadata.DELIMITED_RECORD);
		testMetadata.setFieldDelimiter("|");

		testMetadata.setRecordDelimiter("#\\|#&&&");
		testMetadata.addField(new DataFieldMetadata("Field1", DataFieldMetadata.STRING_FIELD, null));
	
		DataRecord record = DataRecordFactory.newRecord(testMetadata);
		record.init();
		TextParserConfiguration parserConf = new TextParserConfiguration();
		parserConf.setMetadata(testMetadata);
		parserConf.setCharset("UTF-8");
		parserConf.setTrim(false);
		parserConf.setTryToMatchLongerDelimiter(true);

		
		DataParser parser = new DataParser(parserConf);
		parser.init();
		parser.setDataSource(FileUtils.getInputStream(null, TEST_FILE_DELIM3));
		
		List<String> parsed = new LinkedList<String>();
		while ((record = parser.getNext(record)) != null) {
			parsed.add(record.getField(0).getValue().toString());
		}
		parser.close();
		
		assertEquals(4, parsed.size());
		assertEquals("text1", parsed.get(0));
		assertEquals("text2", parsed.get(1));
		assertEquals("text3", parsed.get(2));
		assertEquals("&&a", parsed.get(3));
	}

	public void testFieldDelimiters_substring() throws Exception {
		DataRecordMetadata testMetadata = new DataRecordMetadata("meta", DataRecordMetadata.DELIMITED_RECORD);
		testMetadata.setFieldDelimiter("#\\|#&&&");

		testMetadata.setRecordDelimiter("\r\n");
		testMetadata.addField(new DataFieldMetadata("Field1", DataFieldMetadata.STRING_FIELD, null));
		testMetadata.addField(new DataFieldMetadata("Field2", DataFieldMetadata.STRING_FIELD, null));
		testMetadata.addField(new DataFieldMetadata("Field3", DataFieldMetadata.STRING_FIELD, null));
		testMetadata.addField(new DataFieldMetadata("Field4", DataFieldMetadata.STRING_FIELD, null));
		
		DataRecord record = DataRecordFactory.newRecord(testMetadata);
		record.init();
		TextParserConfiguration parserConf = new TextParserConfiguration();
		parserConf.setMetadata(testMetadata);
		parserConf.setCharset("UTF-8");
		parserConf.setTrim(false);
		parserConf.setTryToMatchLongerDelimiter(true);
		
		DataParser parser = new DataParser(parserConf);
		parser.init();
		parser.setDataSource(FileUtils.getInputStream(null, TEST_FILE_DELIM3_FIELDS));
		
		List<String> parsed = new LinkedList<String>();
		record = parser.getNext(record);
		for (int i=0; i < record.getNumFields(); i++) {
			parsed.add(record.getField(i).getValue().toString());
		}
		parser.close();
		
		assertEquals(4, parsed.size());
		assertEquals("text1", parsed.get(0));
		assertEquals("text2", parsed.get(1));
		assertEquals("text3", parsed.get(2));
		assertEquals("&&a", parsed.get(3));
	}
	
	
	@SuppressWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
	public void testOddBufferSize() throws Exception {
		Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE = 15;
		testParsers();
	}
	
	@SuppressWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
	public void testEvenBufferSize() throws Exception {
		Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE = 16;
		testParsers();
	}
	
	@Override
	@SuppressWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
	protected void tearDown() throws Exception {
		super.tearDown();
		Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE = oldBufferSize;
	}

	@Override
	protected Parser createParser() throws Exception {
		TextParserConfiguration cfg = new TextParserConfiguration(metadata);
		return new DataParser(cfg);
	}
}
