package org.jetel.lookup;

import java.io.File;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.jetel.data.DataRecord;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
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
    private final static String FIELD_START = "start"; 
    private final static String FIELD_DATE = "date";
    private static final String FIELD_INVALID = "invalid";
    private final static String FILE_NAME = "data";
	private final static String DATA_FILE_EXTENSION = ".db";
	private final static String LOG_FILE_EXTENSION = ".lg";
	
    private RecordKey lookupKey;
    private DataRecord dataRecord;
	
	private PersistentLookupTable lookupTable;
	private DataRecordMetadata lookupMetadata;
	
	// record used in testSeek... - it is concrete DataRecord   
	private DataRecord testRecord = null;
	
	protected void setUp() throws Exception {
		initEngine();
		
		// create metadata
		lookupMetadata = new DataRecordMetadata("lookupTest", DataRecordMetadata.DELIMITED_RECORD);
		lookupMetadata.addField(new DataFieldMetadata(FIELD_NAME, DataFieldMetadata.STRING_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata(FIELD_START, DataFieldMetadata.INTEGER_FIELD, ";"));
		DataFieldMetadata fieldMetadata = new DataFieldMetadata(FIELD_DATE, DataFieldMetadata.DATE_FIELD, ";");
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
	
	/**
     * Tests whether the {@link PersistentLookupTable#checkConfig(ConfigurationStatus)} works correctly. This method
     * works correctly if it doesn't throw any exception, doesn't produce any configuration problem and handles
     * correctly invalid values passed to any constructor or set by any of the set*() methods.
     */
    public void testCheckConfig() {
        ConfigurationStatus configurationStatus = new ConfigurationStatus();

        // perform test for correct data
        lookupTable.checkConfig(configurationStatus);
        assertTrue("An error occured even though the provided data was correct!", configurationStatus.isEmpty());

        // perform tests on null values (null values are not permitted)
        lookupTable = new PersistentLookupTable(LOOKUP_TABLE_ID, (String)null, new String[] {FIELD_NAME}, FILE_NAME);
        configurationStatus.clear();
        lookupTable.checkConfig(configurationStatus);
        assertFalse("No configuration problem reported even though the metada is null!",
                configurationStatus.isEmpty());

        lookupTable = new PersistentLookupTable(LOOKUP_TABLE_ID, (DataRecordMetadata)null, new String[] {FIELD_NAME}, FILE_NAME);
        configurationStatus.clear();
        lookupTable.checkConfig(configurationStatus);
        assertFalse("No configuration problem reported even though the metada is null!",
                configurationStatus.isEmpty());

        lookupTable = new PersistentLookupTable(LOOKUP_TABLE_ID, lookupMetadata, null, FILE_NAME);
        configurationStatus.clear();
        lookupTable.checkConfig(configurationStatus);
        assertFalse("No configuration problem reported even though the lookup key field is null!",
                configurationStatus.isEmpty());

        lookupTable = new PersistentLookupTable(LOOKUP_TABLE_ID, lookupMetadata, new String[] {FIELD_NAME}, null);
        configurationStatus.clear();
        lookupTable.checkConfig(configurationStatus);
        assertFalse("No configuration problem reported even though the data file URL is null!",
                configurationStatus.isEmpty());

        // perform tests on invalid values (invalid values are not permitted)
        lookupTable = new PersistentLookupTable(LOOKUP_TABLE_ID, lookupMetadata, new String[] {FIELD_INVALID}, FILE_NAME);
        configurationStatus.clear();
        lookupTable.checkConfig(configurationStatus);
        assertFalse("No configuration problem reported even though the lookup key field is invalid!",
                configurationStatus.isEmpty());
    }
    
    /**
     * Tests whether the {@link PersistentLookupTable#init()} method finishes correctly on a consistent Persistent lookup table.
     */
    public void testInit() {
        lookupTable.checkConfig(new ConfigurationStatus());

        try {
            lookupTable.init();
        } catch (ComponentNotReadyException exception) {
            fail("Initialization of the lookup table failed!");
        }
    }

    /**
     * Tests whether the {@link PersistentLookupTable#put(DataRecord)} method works correctly.
     */
    public void testPut() {
        testInit();

        assertTrue("The put operation is not supported!", lookupTable.isPutSupported());

        dataRecord.getField(FIELD_NAME).setValue("Křižíkova");
        lookupTable.put(dataRecord);

        Lookup persistentLookup = lookupTable.createLookup(lookupKey, dataRecord);
        persistentLookup.seek();

        assertNotNull("No data record found for an existing lookup key!", persistentLookup.next());
    }

    /**
     * Tests whether the {@link PersistentLookupTable#remove(DataRecord)} and {@link PersistentLookupTable#remove(HashKey)}
     * methods are supported and work correctly.
     */
    public void testRemove() {
        testInit();

        assertTrue("The remove operation is not supported!", lookupTable.isRemoveSupported());

        try {
            assertFalse("A data record removed for an non existing lookup key!", 
            		lookupTable.remove((DataRecord) dataRecord));
        } catch (UnsupportedOperationException exception) {
        	fail("The remove(DataRecord) method is not supported although the isRemoveSupported() method returns true!");
        }

        try {
        	assertFalse("A data record removed for an non existing lookup key!", 
        			lookupTable.remove((HashKey) new HashKey(new RecordKey(new int[0], null), dataRecord)));
        } catch (UnsupportedOperationException exception) {
        	fail("The remove(HashKey) method is not supported although the isRemoveSupported() method returns true!");
        }
        
        
        dataRecord.getField(FIELD_NAME).setValue("name");
        
        lookupTable.put(dataRecord);
        assertTrue("A data record not removed for an existing lookup key!", lookupTable.remove(dataRecord));
        
        Lookup persistentLookup = lookupTable.createLookup(lookupKey, dataRecord);
        persistentLookup.seek();

        while (persistentLookup.hasNext()) {
        	assertNull("A data record found for an non existing lookup key!", persistentLookup.next());
        }
        
     
        lookupTable.put(dataRecord);
        lookupTable.remove(new HashKey(lookupKey, dataRecord));
        
        persistentLookup = lookupTable.createLookup(lookupKey, dataRecord);
        persistentLookup.seek();

        while (persistentLookup.hasNext()) {
        	assertNull("A data record found for an non existing lookup key!", persistentLookup.next());
        }
    }

    /**
     * Tests whether the {@link PersistentLookupTable#createLookup(RecordKey)} and
     * {@link PersistentLookupTable#createLookup(RecordKey, DataRecord)} methods work correctly. The returned lookup
     * proxy object should returns multiple data records for slightly misspelled lookup keys and no data records
     * for non-existing lookup keys.
     */
    public void testCreateLookup() {
        testInit();

        // fill lookup table
        dataRecord.getField(FIELD_NAME).setValue("Panenská");
        lookupTable.put(dataRecord);
        
        // test whether the methods create and initialize the lookup proxy object correctly
        Lookup persistentLookup = lookupTable.createLookup(lookupKey);

        assertFalse("The Lookup.hasNext() method returned true even though the data record was not set!",
        		persistentLookup.hasNext());

        try {
        	persistentLookup.next();
            fail("The Lookup.next() method did work even though the data record was not set!");
        } catch (NoSuchElementException exception) {
        }

        try {
        	persistentLookup.seek();
            fail("The Lookup.seek() method did work even though the data record was not set!");
        } catch (IllegalStateException exception) {
        }

        // perform tests on existing lookup keys
        dataRecord.getField(FIELD_NAME).setValue("Panenská");
        persistentLookup.seek(dataRecord);
        assertTrue("No data records found for an existing key!", persistentLookup.hasNext());
        assertEquals("The number of returned data records is invalid!", 1, persistentLookup.getNumFound());

        persistentLookup.next();

        assertFalse("The number of data records returned by the iterator is invalid!", persistentLookup.hasNext());

        try {
        	persistentLookup.next();
            fail("The Lookup.next() method returned another data record even though there should not be any!");
        } catch (NoSuchElementException exception) {
        }

        // perform tests on non-existing lookup keys
        dataRecord.getField(FIELD_NAME).setValue("Křižíkova");
        persistentLookup.seek(dataRecord);
        assertFalse("A data record found for a non-existing key!", persistentLookup.hasNext());
        assertEquals("The number of returned data records is invalid!", 0, persistentLookup.getNumFound());

        dataRecord.getField(FIELD_NAME).setValue("kuk");
        persistentLookup.seek(dataRecord);
        assertFalse("A data record found for a non-existing key!", persistentLookup.hasNext());
        assertEquals("The number of returned data records is invalid!", 0, persistentLookup.getNumFound());
    }

    /**
     * Tests whether the iterator returned by the {@link PersistentLookupTable#iterator()} method correctly returns all
     * the data records stored in the lookup table.
     */
    public void testIterator() {
        testInit();
        
        // the data records that should be contained in the lookup table
        Set<DataRecord> lookupTableDataRecords = getTestRecords();
        
        // fill the lookup table
        for (DataRecord tmpRecord : lookupTableDataRecords) {
			lookupTable.put(tmpRecord);
		}

        // count the data records and check their content
        int dataRecordsCount = lookupTableDataRecords.size();
        
        int numFoundTestRecord = 0;
        for (DataRecord dataRecord : lookupTable) {
        	if (testRecord.equals(dataRecord)) {
        		numFoundTestRecord++;
        	}
            lookupTableDataRecords.remove(dataRecord);
            dataRecordsCount--;
        }

        assertEquals("The iterator didn't return test data record!", 1, numFoundTestRecord);
        assertEquals("The iterator returned invalid number of data records!", 0, dataRecordsCount);
        assertTrue("The iterator did not return all the data records!", lookupTableDataRecords.isEmpty());

        // test the behaviour of the next() method when there are no more data records
        Iterator<DataRecord> iterator = lookupTable.iterator();

        while (iterator.hasNext()) {
            iterator.next();
        }

        try {
            iterator.next();
            fail("The next() method returned another data record even though there should not be any!");
        } catch (NoSuchElementException exception) {
        }
    }

    /**
     * Tests whether the {@link PersistentLookupTable#reset()} method resets the state of the lookup table correctly.
     */
    public void testReset() {
        testInit();

        try {
            lookupTable.reset();
        } catch (ComponentNotReadyException exception) {
            fail("Reseting of the lookup table failed!");
        }

        ConfigurationStatus configurationStatus = new ConfigurationStatus();

        lookupTable.checkConfig(configurationStatus);
        assertTrue("A configuration problem occured even though the reset() method was called!",
                configurationStatus.isEmpty());
    }

    /**
     * Tests whether the {@link PersistentLookup#seek()} returns correct data when as input data is used
     * data record with different lookup key.
     */
    public void testSeekDifferentLookupKey() {
    	testInit();
    	fillLookupTable();
    	
    	DataRecordMetadata recMet = new DataRecordMetadata("rec", DataRecordMetadata.DELIMITED_RECORD);
        recMet.addField(new DataFieldMetadata("in", DataFieldMetadata.INTEGER_FIELD, ";"));
        recMet.addField(new DataFieldMetadata("jmeno2", DataFieldMetadata.STRING_FIELD, ";"));
        
        RecordKey lookupKey2 = new RecordKey(new String[] {"jmeno2"}, recMet);
        lookupKey2.init();
        
        DataRecord rec = new DataRecord(recMet);
        rec.init();
        rec.getField("jmeno2").setValue(testRecord.getField(FIELD_NAME));
        
        Lookup persistentLookup = lookupTable.createLookup(lookupKey2);
       	persistentLookup.seek(rec);
       	
       	assertTrue("No data records found for an existing key!", persistentLookup.hasNext());
        assertEquals("The number of returned data records is invalid!", 1, persistentLookup.getNumFound());
        assertEquals("The returned record isn't correct!", testRecord, persistentLookup.next());
        assertFalse("The number of data records returned by the iterator is invalid!", persistentLookup.hasNext());
    }
    
    /**
     * Tests whether the {@link PersistentLookupTable#remove(DataRecord)} and {@link PersistentLookupTable#remove(HashKey)}
     * methods work correctly with different lookup key.
     */
    public void testRemoveDifferentLookupKey() {
    	testInit();
    	fillLookupTable();
    	
    	DataRecordMetadata recMet = new DataRecordMetadata("rec", DataRecordMetadata.DELIMITED_RECORD);
        recMet.addField(new DataFieldMetadata("in", DataFieldMetadata.INTEGER_FIELD, ";"));
        recMet.addField(new DataFieldMetadata("inin", DataFieldMetadata.INTEGER_FIELD, ";"));
        recMet.addField(new DataFieldMetadata("ininin", DataFieldMetadata.INTEGER_FIELD, ";"));
        recMet.addField(new DataFieldMetadata("jmeno2", DataFieldMetadata.STRING_FIELD, ";"));
        
        RecordKey lookupKey2 = new RecordKey(new String[] {"jmeno2"}, recMet);
        lookupKey2.init();
        
        DataRecord rec = new DataRecord(recMet);
        rec.init();
        rec.getField("jmeno2").setValue(testRecord.getField(FIELD_NAME));

        int initRecordCount = getRecordCount();
        
        try {
        	lookupTable.remove(rec);
            fail("A data record was removed for an lookup key although lookup different key wasn't set!");
        } catch (Exception e) {
		}
        
        assertEquals("The number of returned data records is invalid!", initRecordCount, getRecordCount());
        
        assertTrue("A data record wasn't removed for an lookup key although lookup key was set!",
        		lookupTable.remove(new HashKey(lookupKey2, rec)));
        
        assertEquals("The number of returned data records is invalid!", initRecordCount - 1, getRecordCount());
    }
    
    /**
     * Tests whether the {@link PersistentLookupTable#put(DataRecord)} 
     * methods work correctly with different lookup key.
     */
    public void testPutDifferentLookupKey() {
    	testInit();
    	
    	DataRecordMetadata recMet = new DataRecordMetadata("rec", DataRecordMetadata.DELIMITED_RECORD);
        recMet.addField(new DataFieldMetadata("in", DataFieldMetadata.INTEGER_FIELD, ";"));
        recMet.addField(new DataFieldMetadata("inin", DataFieldMetadata.INTEGER_FIELD, ";"));
        recMet.addField(new DataFieldMetadata("ininin", DataFieldMetadata.INTEGER_FIELD, ";"));
        recMet.addField(new DataFieldMetadata("jmeno2", DataFieldMetadata.STRING_FIELD, ";"));
        
        RecordKey lookupKey2 = new RecordKey(new String[] {"jmeno2"}, recMet);
        lookupKey2.init();

        // only for set testRecord
        getTestRecords();
        
        DataRecord rec = new DataRecord(recMet);
        rec.init();
        rec.getField("jmeno2").setValue(testRecord.getField(FIELD_NAME));

        // set differnet lookup key
        assertFalse("A data record was removed for an lookup key although no record is in!", 
        		lookupTable.remove(new HashKey(lookupKey2, rec)));
        
       	assertTrue("A data record wasn't put to an lookup key!", lookupTable.put(testRecord));
    }
    
    private int getRecordCount() {
    	int recordCount = 0;
        for (@SuppressWarnings("unused") DataRecord record : lookupTable) {
			recordCount++;
		}
        return recordCount;
    }
    
    private void fillLookupTable() {
    	Set<DataRecord> lookupTableDataRecords = getTestRecords();
        
        // fill the lookup table
        for (DataRecord tmpRecord : lookupTableDataRecords) {
			lookupTable.put(tmpRecord);
		}
    }
    
	private Set<DataRecord> getTestRecords() {
		Set<DataRecord> lookupTableDataRecords = new HashSet<DataRecord>();
		DataRecord record;
		record = new DataRecord(lookupMetadata);
		record.init();
		
		Calendar cal = Calendar.getInstance();
		
		record.getField("name").setValue("10-20");
		record.getField("start").setValue(10);
		record.getField("date").setValue(cal.getTime());
		lookupTableDataRecords.add(record.duplicate());
		
		record.getField("name").setValue("15-20");
		record.getField("start").setValue(15);
		record.getField("date").setValue(cal.getTime());
		lookupTableDataRecords.add(record.duplicate());
		
		record.getField("name").setValue("20-25");
		record.getField("start").setValue(20);
		record.getField("date").setValue(cal.getTime());
		testRecord = record.duplicate();
		lookupTableDataRecords.add(record.duplicate());
		
		record.getField("name").setValue("20-30");
		record.getField("start").setValue(20);
		record.getField("date").setValue(cal.getTime());
		lookupTableDataRecords.add(record.duplicate());
		
		record.getField("name").setValue("30-40");
		record.getField("start").setValue(30);
		record.getField("date").setValue(cal.getTime());
		lookupTableDataRecords.add(record.duplicate());
		
		record.getField("name").setValue("0-10");
		record.getField("start").setValue(0);
		record.getField("date").setValue(cal.getTime());
		lookupTableDataRecords.add(record.duplicate());
		
		return lookupTableDataRecords;
	}
}