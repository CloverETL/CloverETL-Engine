package org.jetel.lookup;

import java.io.File;
import java.util.Calendar;

import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.LookupTableFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;


public class PersistentLookupTest extends CloverTestCase {

    private final static String LOOKUP_TABLE_ID = "PersistentLookup";
    private final static String LOOKUP_TABLE_TYPE = "persistentLookup";
    private final static String FIELD_NAME = "name";
	private final static String FILE_NAME = "data";
	private final static String DATA_FILE_EXTENSION = ".db";
	private final static String LOG_FILE_EXTENSION = ".lg";
	
    private RecordKey lookupKey;
    private DataRecord dataRecord;
	
	private PersistentLookupTable lookupTable;
	private DataRecordMetadata lookupMetadata;
	
	private DataRecord recordToFound1;
	private DataRecord recordToFound2;
	private DataRecord recordToFound3;
	
	protected void setUp() throws Exception {
		initEngine();
		
		// create metadata
		lookupMetadata = new DataRecordMetadata("lookupTest", DataRecordMetadata.DELIMITED_RECORD);
		lookupMetadata.addField(new DataFieldMetadata(FIELD_NAME, DataFieldMetadata.STRING_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("start", DataFieldMetadata.INTEGER_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("end", DataFieldMetadata.INTEGER_FIELD, ";"));
		DataFieldMetadata fieldMetadata = new DataFieldMetadata("date", DataFieldMetadata.DATE_FIELD, ";");
		fieldMetadata.setFormatStr("dd.MM.yy");
		lookupMetadata.addField(fieldMetadata);
		
		// create lookup table
		lookupTable = (PersistentLookupTable) LookupTableFactory.createLookupTable(null, LOOKUP_TABLE_TYPE, 
				new Object[] { LOOKUP_TABLE_ID, lookupMetadata, new String[] {FIELD_NAME}, FILE_NAME }, 
				new Class[] { String.class, DataRecordMetadata.class, String[].class, String.class });
		
		lookupKey = new RecordKey(new String[] {FIELD_NAME}, lookupMetadata);
        lookupKey.init();
        dataRecord = new DataRecord(lookupMetadata);
        dataRecord.init();
	}

	protected void tearDown() throws Exception {
		lookupTable.free();
		deleteDataFile();
	}
	
	private static void deleteDataFile() {
		File file = new File(FILE_NAME + DATA_FILE_EXTENSION);
		file.delete();

		file = new File(FILE_NAME + LOG_FILE_EXTENSION);
		file.delete();
	}
	
//	/**
//     * Tests whether the {@link PersistentLookupTable#checkConfig(ConfigurationStatus)} works correctly. This method
//     * works correctly if it doesn't throw any exception, doesn't produce any configuration problem and handles
//     * correctly invalid values passed to any constructor or set by any of the set*() methods.
//     */
//    public void testCheckConfig() {
//        ConfigurationStatus configurationStatus = new ConfigurationStatus();
//
//        // perform test for correct data
//        lookupTable.checkConfig(configurationStatus);
//        assertTrue("An error occured even though the provided data was correct!", configurationStatus.isEmpty());
//
//        // perform tests on null values (null values are not permitted)
//        lookupTable = new PersistentLookupTable(LOOKUP_TABLE_ID, (String)null, new String[] {FIELD_NAME}, FILE_NAME);
//        configurationStatus.clear();
//        lookupTable.checkConfig(configurationStatus);
//        assertFalse("No configuration problem reported even though the metada is null!",
//                configurationStatus.isEmpty());
//
//        lookupTable = new PersistentLookupTable(LOOKUP_TABLE_ID, (DataRecordMetadata)null, new String[] {FIELD_NAME}, FILE_NAME);
//        configurationStatus.clear();
//        lookupTable.checkConfig(configurationStatus);
//        assertFalse("No configuration problem reported even though the metada is null!",
//                configurationStatus.isEmpty());
//
//        
//        lookupTable = new AspellLookupTable(LOOKUP_TABLE_ID, metadata, null, DATA_FILE_URL);
//        configurationStatus.clear();
//        lookupTable.checkConfig(configurationStatus);
//        assertFalse("No configuration problem reported even though the lookup key field is null!",
//                configurationStatus.isEmpty());
//
//        lookupTable = new AspellLookupTable(LOOKUP_TABLE_ID, metadata, FIELD_STREET, null);
//        configurationStatus.clear();
//        lookupTable.checkConfig(configurationStatus);
//        assertFalse("No configuration problem reported even though the data file URL is null!",
//                configurationStatus.isEmpty());
//
//        //
//        // perform tests on invalid values (invalid values are not permitted)
//        //
//
//        lookupTable = new AspellLookupTable(LOOKUP_TABLE_ID, metadata, FIELD_INVALID, DATA_FILE_URL);
//        configurationStatus.clear();
//        lookupTable.checkConfig(configurationStatus);
//        assertFalse("No configuration problem reported even though the lookup key field is invalid!",
//                configurationStatus.isEmpty());
//
//        lookupTable = new AspellLookupTable(LOOKUP_TABLE_ID, metadata, FIELD_STREET, DATA_FILE_URL_INVALID);
//        configurationStatus.clear();
//        lookupTable.checkConfig(configurationStatus);
//        assertFalse("No configuration problem reported even though the data file URL is invalid!",
//                configurationStatus.isEmpty());
//
//        lookupTable = new AspellLookupTable(LOOKUP_TABLE_ID, metadata, FIELD_STREET, DATA_FILE_URL);
//        lookupTable.setDataFileCharset(DATA_FILE_CHARSET_INVALID);
//        configurationStatus.clear();
//        lookupTable.checkConfig(configurationStatus);
//        assertFalse("No configuration problem reported even though the data file charset is invalid!",
//                configurationStatus.isEmpty());
//
//        lookupTable = new AspellLookupTable(LOOKUP_TABLE_ID, metadata, FIELD_STREET, DATA_FILE_URL);
//        lookupTable.setSpellingThreshold(SPELLING_THRESHOLD_INVALID);
//        configurationStatus.clear();
//        lookupTable.checkConfig(configurationStatus);
//        assertFalse("No configuration problem reported even though the spelling threshold is invalid!",
//                configurationStatus.isEmpty());
//    }
//	
//	/**
//	 * use only one field as a key
//	 * @throws ComponentNotReadyException
//	 */
//	public void testGet() throws ComponentNotReadyException {
//		// create lookup table
//		lookupTable = (PersistentLookupTable) LookupTableFactory.createLookupTable(null, LOOKUP_TABLE_TYPE, 
//				new Object[] { LOOKUP_TABLE_ID, lookupMetadata, new String[] {FIELD_NAME}, FILE_NAME }, 
//				new Class[] { String.class, DataRecordMetadata.class, String[].class, String.class });
//		ConfigurationStatus status = new ConfigurationStatus();
//		lookupTable.checkConfig(status);
//		lookupTable.init();
//		
//		// add records and set records to found
//		addRecords();
//
//		// run test		
//		doTestGetDataRecord(recordToFound1);
//		doTestGetDataRecord(recordToFound2);
//		doTestGetDataRecord(recordToFound3);
//		
//		doTestGetObject(recordToFound1);
//		doTestGetObject(recordToFound2);
//		doTestGetObject(recordToFound3);
//		
//		doTestGetString(recordToFound1);
//		doTestGetString(recordToFound2);
//		doTestGetString(recordToFound3);
//	}
//	
//	/**
//	 * use only one field as a key
//	 * use method: get(DataRecord keyRecord)
//	 */
//	private void doTestGetDataRecord(DataRecord recordToFound) {
////		DataRecord record;
////		record = new DataRecord(lookupMetadata);
////		record.init();
////
////		Object valueOfKey = recordToFound.getField("name").getValue();
////		record.getField("name").setValue(valueOfKey);
////		DataRecord foundRecord = lookupTable.get(record);
////		assertTrue(recordToFound.equals(foundRecord));
//	}
//	
//	/**
//	 * use only one field as a key
//	 * use method: get(Object[] keys)
//	 */
//	private void doTestGetObject(DataRecord recordToFound) {
////		Object valueOfKey = recordToFound.getField("name").getValue();
////		Object[] key = new Object[] { valueOfKey };
////		DataRecord foundRecord = lookupTable.get(key);
////		assertTrue(recordToFound.equals(foundRecord));
//	}
//	
//	/**
//	 * use only one field as a key
//	 * use method: get(String keyString)
//	 */
//	private void doTestGetString(DataRecord recordToFound) {
////		Object valueOfKey = recordToFound.getField("name").getValue();
////		String key = ((StringBuilder)valueOfKey).toString();
////		DataRecord foundRecord = lookupTable.get(key);
////		assertTrue(recordToFound.equals(foundRecord));
//	}
//	
//	/**
//	 * use two fields as a key
//	 * @throws ComponentNotReadyException
//	 */
//	public void testGet2() throws ComponentNotReadyException {
//		// create lookup table
//		lookupTable = (PersistentLookupTable) LookupTableFactory.createLookupTable(null, LOOKUP_TABLE_TYPE, 
//				new Object[] { LOOKUP_TABLE_ID, lookupMetadata, new String[] {FIELD_NAME, "start"}, FILE_NAME }, 
//				new Class[] { String.class, DataRecordMetadata.class, String[].class, String.class });
//		ConfigurationStatus status = new ConfigurationStatus();
//		lookupTable.checkConfig(status);
//		lookupTable.init();
//		
//		// add records
//		addRecords();
//
//		// run test		
//		doTestGet2DataRecord(recordToFound1);
//		doTestGet2DataRecord(recordToFound2);
//		doTestGet2DataRecord(recordToFound3);
//		
//		doTestGetObject2(recordToFound1);
//		doTestGetObject2(recordToFound2);
//		doTestGetObject2(recordToFound3);
//		
//		doTestGet2Fail(recordToFound1);
//		doTestGet2Fail(recordToFound2);
//		doTestGet2Fail(recordToFound3);
//	}
//	
//	/**
//	 * use two fields field as a key
//	 * use method: get(DataRecord keyRecord)
//	 */
//	private void doTestGet2DataRecord(DataRecord recordToFound) {
////		DataRecord record;
////		record = new DataRecord(lookupMetadata);
////		record.init();
////
////		Object valueOfKey1 = recordToFound.getField("name").getValue();
////		Object valueOfKey2 = recordToFound.getField("start").getValue();
////		record.getField("name").setValue(valueOfKey1);
////		record.getField("start").setValue(valueOfKey2);
////		DataRecord foundRecord = lookupTable.get(record);
////		assertTrue(recordToFound.equals(foundRecord));
//	}
//	
//	/**
//	 * use two fields as a key
//	 * use method: get(Object[] keys)
//	 */
//	private void doTestGetObject2(DataRecord recordToFound) {
////		Object valueOfKey1 = recordToFound.getField("name").getValue();
////		Object valueOfKey2 = recordToFound.getField("start").getValue();
////		Object[] key = new Object[] { valueOfKey1, valueOfKey2 };
////		DataRecord foundRecord = lookupTable.get(key);
////		assertTrue(recordToFound.equals(foundRecord));
//	}
//	
//	/**
//	 * should be use two fields field as a key
//	 */
//	private void doTestGet2Fail(DataRecord recordToFound) {
////		// skip records with zero field - unspecified field is equal zero field
////		if ( ((Integer)(recordToFound.getField("start").getValue())).equals(new Integer(0))) {
////			return;
////		}
////		DataRecord record;
////		record = new DataRecord(lookupMetadata);
////		record.init();
////
////		Object valueOfKey1 = recordToFound.getField("name").getValue();
////		record.getField("name").setValue(valueOfKey1);
////		DataRecord foundRecord = lookupTable.get(record);
////		assertNull(foundRecord);
//	}
//
//	private void addRecords() {
//		DataRecord record;
//		record = new DataRecord(lookupMetadata);
//		record.init();
//		
//		Calendar cal = Calendar.getInstance();
//		
//		record.getField("name").setValue("10-20");
//		record.getField("start").setValue(10);
//		record.getField("end").setValue(20);
//		record.getField("date").setValue(cal.getTime());
//		lookupTable.put(record);
//		recordToFound1 = record.duplicate();
//		
//		record.getField("name").setValue("15-20");
//		record.getField("start").setValue(15);
//		record.getField("end").setValue(20);
//		record.getField("date").setValue(cal.getTime());
//		lookupTable.put(record);
//		
//		record.getField("name").setValue("20-25");
//		record.getField("start").setValue(20);
//		record.getField("end").setValue(25);
//		record.getField("date").setValue(cal.getTime());
//		lookupTable.put(record);
//		
//		record.getField("name").setValue("20-30");
//		record.getField("start").setValue(20);
//		record.getField("end").setValue(30);
//		record.getField("date").setValue(cal.getTime());
//		lookupTable.put(record);
//		recordToFound2 = record.duplicate();
//		
//		record.getField("name").setValue("30-40");
//		record.getField("start").setValue(30);
//		record.getField("end").setValue(40);
//		record.getField("date").setValue(cal.getTime());
//		lookupTable.put(record);
//		
//		record.getField("name").setValue("0-10");
//		record.getField("start").setValue(0);
//		record.getField("end").setValue(10);
//		record.getField("date").setValue(cal.getTime());
//		lookupTable.put(record);
//		recordToFound3 = record.duplicate();
//	}
}