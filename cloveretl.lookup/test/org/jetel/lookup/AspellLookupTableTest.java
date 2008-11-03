/*
 * jETeL/Clover.ETL - Java based ETL application framework.
 * Copyright (C) 2002-2008  David Pavlis <david.pavlis@javlin.cz>
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.lookup;

import java.io.IOException;

import java.util.HashSet;
import java.util.Set;

import org.jetel.data.DataRecord;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.jetel.util.file.FileUtils;

/**
 * Represents a unit test of the <code>AspellLookupTable</code> class.
 *
 * @author Martin Janik <martin.janik@javlin.cz>
 *
 * @version 3rd November 2008
 * @since 27th October 2008
 */
public final class AspellLookupTableTest extends CloverTestCase {

    /** the ID of the lookup table */
    private static final String LOOKUP_TABLE_ID = "id";

    /** the ID of the meta data used by the lookup table */
    private static final String METADATA_ID = "aspell";

    /** the main field used for lookup */
    private static final String FIELD_STREET = "street";
    /** the other field used to test whether the lookup table can handle multiple records per key */
    private static final String FIELD_CITY = "city";
    /** an invalid field used in the {@link #testCheckConfig()} method */
    private static final String FIELD_INVALID = "invalid";

    /** the delimiter used to separate fields */
    private static final String DELIMITER_FIELDS = ";";
    /** the delimiter used to separate data records */
    private static final String DELIMITER_RECORDS = "\n";

    /** the URL of a valid lookup table data file */
    private static final String DATA_FILE_URL = "data/aspell-lookup-table.dat";
    /** an invalid URL of the lookup table data file used in the {@link #testCheckConfig()} method */
    private static final String DATA_FILE_URL_INVALID = "invalid-data-file-url.dat";

    /** the character set used by the lookup table data file */
    private static final String DATA_FILE_CHARSET = "UTF-8";
    /** an invalid character set of the lookup table data file used in the {@link #testCheckConfig()} method */
    private static final String DATA_FILE_CHARSET_INVALID = "invalid-charset";

    /** an invalid spelling threshold used in the {@link #testCheckConfig()} method */
    private static final int SPELLING_THRESHOLD_INVALID = -1;

    /** the meta data used by the lookup table */
    private DataRecordMetadata metadata;
    /** the Aspell lookup table used for testing */
    private AspellLookupTable aspellLookupTable;

    /** the record key used for lookup */
    private RecordKey lookupKey;
    /** the data record used for lookup */
    private DataRecord dataRecord;

    @Override
    protected void setUp() throws Exception {
        initEngine();

        metadata = new DataRecordMetadata(METADATA_ID, DataRecordMetadata.DELIMITED_RECORD);
        metadata.addField(new DataFieldMetadata(FIELD_STREET, DELIMITER_FIELDS));
        metadata.addField(new DataFieldMetadata(FIELD_CITY, DELIMITER_RECORDS));

        aspellLookupTable = new AspellLookupTable(LOOKUP_TABLE_ID, metadata, FIELD_STREET, DATA_FILE_URL);
        aspellLookupTable.setDataFileCharset(DATA_FILE_CHARSET);

        lookupKey = new RecordKey(new String[] {FIELD_STREET}, metadata);
        lookupKey.init();
        dataRecord = new DataRecord(metadata);
        dataRecord.init();
    }

    /**
     * Tests whether the {@link AspellLookupTable#checkConfig(ConfigurationStatus)} works correctly. This method
     * works correctly if it doesn't throw any exception, doesn't produce any configuration problem and handles
     * correctly invalid values passed to any constructor or set by any of the set*() methods.
     */
    public void testCheckConfig() {
        ConfigurationStatus configurationStatus = new ConfigurationStatus();

        //
        // perform test for correct data
        //

        aspellLookupTable.checkConfig(configurationStatus);
        assertTrue("An error occured even though the provided data was correct!", configurationStatus.isEmpty());

        //
        // perform tests on null values (null values are not permitted)
        //

        aspellLookupTable = new AspellLookupTable(LOOKUP_TABLE_ID, null, FIELD_STREET, DATA_FILE_URL);
        configurationStatus.clear();
        aspellLookupTable.checkConfig(configurationStatus);
        assertFalse("No configuration problem reported even though the metada is null!",
                configurationStatus.isEmpty());

        aspellLookupTable = new AspellLookupTable(LOOKUP_TABLE_ID, metadata, null, DATA_FILE_URL);
        configurationStatus.clear();
        aspellLookupTable.checkConfig(configurationStatus);
        assertFalse("No configuration problem reported even though the lookup key field is null!",
                configurationStatus.isEmpty());

        aspellLookupTable = new AspellLookupTable(LOOKUP_TABLE_ID, metadata, FIELD_STREET, null);
        configurationStatus.clear();
        aspellLookupTable.checkConfig(configurationStatus);
        assertFalse("No configuration problem reported even though the data file URL is null!",
                configurationStatus.isEmpty());

        //
        // perform tests on invalid values (invalid values are not permitted)
        //

        aspellLookupTable = new AspellLookupTable(LOOKUP_TABLE_ID, metadata, FIELD_INVALID, DATA_FILE_URL);
        configurationStatus.clear();
        aspellLookupTable.checkConfig(configurationStatus);
        assertFalse("No configuration problem reported even though the lookup key field is invalid!",
                configurationStatus.isEmpty());

        aspellLookupTable = new AspellLookupTable(LOOKUP_TABLE_ID, metadata, FIELD_STREET, DATA_FILE_URL_INVALID);
        configurationStatus.clear();
        aspellLookupTable.checkConfig(configurationStatus);
        assertFalse("No configuration problem reported even though the data file URL is invalid!",
                configurationStatus.isEmpty());

        aspellLookupTable = new AspellLookupTable(LOOKUP_TABLE_ID, metadata, FIELD_STREET, DATA_FILE_URL);
        aspellLookupTable.setDataFileCharset(DATA_FILE_CHARSET_INVALID);
        configurationStatus.clear();
        aspellLookupTable.checkConfig(configurationStatus);
        assertFalse("No configuration problem reported even though the data file charset is invalid!",
                configurationStatus.isEmpty());

        aspellLookupTable = new AspellLookupTable(LOOKUP_TABLE_ID, metadata, FIELD_STREET, DATA_FILE_URL);
        aspellLookupTable.setSpellingThreshold(SPELLING_THRESHOLD_INVALID);
        configurationStatus.clear();
        aspellLookupTable.checkConfig(configurationStatus);
        assertFalse("No configuration problem reported even though the spelling threshold is invalid!",
                configurationStatus.isEmpty());
    }

    /**
     * Tests whether the {@link AspellLookupTable#init()} method finishes correctly on a consistent Aspell lookup table.
     */
    public void testInit() {
        aspellLookupTable.checkConfig(new ConfigurationStatus());

        try {
            aspellLookupTable.init();
        } catch (ComponentNotReadyException exception) {
            fail("Initialization of the lookup table failed!");
        }
    }

    /**
     * Tests whether the {@link AspellLookupTable#put(DataRecord)}, {@link AspellLookupTable#remove(DataRecord)} and
     * {@link AspellLookupTable#remove(HashKey)} methods are not supported.
     */
    public void testPutAndRemove() {
        testInit();

        assertTrue("The lookup table is not read-only!", aspellLookupTable.isReadOnly());

        try {
            aspellLookupTable.put(null);
            fail("The put(DataRecord) method is supported although the lookup table is read-only!");
        } catch (UnsupportedOperationException exception) {
        }

        try {
            aspellLookupTable.remove((DataRecord) null);
            fail("The remove(DataRecord) method is supported although the lookup table is read-only!");
        } catch (UnsupportedOperationException exception) {
        }

        try {
            aspellLookupTable.remove((HashKey) null);
            fail("The remove(HashKey) method is supported although the lookup table is read-only!");
        } catch (UnsupportedOperationException exception) {
        }
    }

    /**
     * Tests whether the {@link AspellLookupTable#get(RecordKey, DataRecord)} returns some data records for slightly
     * misspelled lookup keys and no data records for non-existing lookup keys.
     */
    public void testGet() {
        testInit();

        //
        // perform tests on existing lookup keys
        //

        dataRecord.getField(FIELD_STREET).setValue("Panenská");
        assertNotNull("No data record found for an existing lookup key!", aspellLookupTable.get(lookupKey, dataRecord));

        dataRecord.getField(FIELD_STREET).setValue("miselov");
        assertNotNull("No data record found for an existing lookup key!", aspellLookupTable.get(lookupKey, dataRecord));

        dataRecord.getField(FIELD_STREET).setValue("francuzska");
        assertNotNull("No data record found for an existing lookup key!", aspellLookupTable.get(lookupKey, dataRecord));

        dataRecord.getField(FIELD_STREET).setValue("Wolkerova");
        assertNotNull("No data record found for an existing lookup key!", aspellLookupTable.get(lookupKey, dataRecord));

        //
        // perform tests on non-existing lookup keys
        //

        dataRecord.getField(FIELD_STREET).setValue("Křižíkova");
        assertNull("A data record found for a non-existing lookup key!", aspellLookupTable.get(lookupKey, dataRecord));

        dataRecord.getField(FIELD_STREET).setValue("Belgická");
        assertNull("A data record found for a non-existing lookup key!", aspellLookupTable.get(lookupKey, dataRecord));
    }

    /**
     * Tests whether the deprecated methods {@link AspellLookupTable#getNext()} and {@link AspellLookupTable#getNumFound()}
     * work correctly.
     */
    @SuppressWarnings("deprecation")
    public void testGetNextAndGetNumFound() {
        testInit();

        //
        // perform test on an existing lookup key
        //

        dataRecord.getField(FIELD_STREET).setValue("Údolní");

        DataRecord matchingDataRecord = aspellLookupTable.get(lookupKey, dataRecord);
        int dataRecordsCount = 0;

        while (matchingDataRecord != null) {
            matchingDataRecord = aspellLookupTable.getNext();
            dataRecordsCount++;
        }

        assertEquals("The number of found data records is invalid!", 3, dataRecordsCount);
        assertEquals("The returned number of data records differs from the number of data records actually returned!",
                dataRecordsCount, aspellLookupTable.getNumFound());

        //
        // perform test on a non-existing lookup key
        //

        dataRecord.getField(FIELD_STREET).setValue("Vídeňská");
        aspellLookupTable.get(lookupKey, dataRecord);

        assertNull("A data record returned for a non-existing key!", aspellLookupTable.getNext());
        assertEquals("The number of found data records is invalid!", 0, aspellLookupTable.getNumFound());
    }

    /**
     * Tests whether the {@link AspellLookupTable#createLookup(RecordKey)} and
     * {@link AspellLookupTable#createLookup(RecordKey, DataRecord)} methods work correctly. The returned lookup
     * proxy object should returns multiple data records for slightly misspelled lookup keys and no data records
     * for non-existing lookup keys.
     */
    public void testCreateLookup() {
        testInit();

        //
        // test whether the methods create and initialize the lookup proxy object correctly
        //

        Lookup aspellLookup = aspellLookupTable.createLookup(lookupKey);

        try {
            aspellLookup.seek();
            fail("The Lookup.seek() method did work although the data record was not set!");
        } catch (IllegalStateException exception) {
        }

        //
        // perform tests on existing lookup keys
        //

        dataRecord.getField(FIELD_STREET).setValue("Panenská");
        aspellLookup.seek(dataRecord);
        assertTrue("No data records found for an existing key!", aspellLookup.hasNext());
        assertEquals("The number of returned data records is invalid!", 3, aspellLookup.getNumFound());

        aspellLookup.next();
        aspellLookup.next();
        aspellLookup.next();

        assertFalse("The number of data records returned by the iterator is invalid!", aspellLookup.hasNext());

        dataRecord.getField(FIELD_STREET).setValue("Elišky krásnohorské");
        aspellLookup.seek(dataRecord);
        assertTrue("No data records found for an existing key!", aspellLookup.hasNext());
        assertEquals("The number of returned data records is invalid!", 3, aspellLookup.getNumFound());

        aspellLookup.next();
        aspellLookup.next();
        aspellLookup.next();

        assertFalse("The number of data records returned by the iterator is invalid!", aspellLookup.hasNext());

        dataRecord.getField(FIELD_STREET).setValue("husova");
        aspellLookup.seek(dataRecord);
        assertTrue("No data records found for an existing key!", aspellLookup.hasNext());
        assertEquals("The number of returned data records is invalid!", 4, aspellLookup.getNumFound());

        aspellLookup.next();
        aspellLookup.next();
        aspellLookup.next();
        aspellLookup.next();

        assertFalse("The number of data records returned by the iterator is invalid!", aspellLookup.hasNext());

        dataRecord.getField(FIELD_STREET).setValue("orli");
        aspellLookup.seek(dataRecord);
        assertTrue("No data records found for an existing key!", aspellLookup.hasNext());
        assertEquals("The number of returned data records is invalid!", 1, aspellLookup.getNumFound());

        aspellLookup.next();

        assertFalse("The number of data records returned by the iterator is invalid!", aspellLookup.hasNext());

        //
        // perform tests on non-existing lookup keys
        //

        dataRecord.getField(FIELD_STREET).setValue("Křižíkova");
        aspellLookup.seek(dataRecord);
        assertFalse("A data record found for a non-existing key!", aspellLookup.hasNext());
        assertEquals("The number of returned data records is invalid!", 0, aspellLookup.getNumFound());

        dataRecord.getField(FIELD_STREET).setValue("Belgická");
        aspellLookup.seek(dataRecord);
        assertFalse("A data record found for a non-existing key!", aspellLookup.hasNext());
        assertEquals("The number of returned data records is invalid!", 0, aspellLookup.getNumFound());
    }

    /**
     * Tests whether the iterator returned by the {@link AspellLookupTable#iterator()} method correctly returns all
     * the data records stored in the lookup table.
     */
    public void testIterator() {
        testInit();

        //
        // load the data records that should be contained in the lookup table
        //

        Set<DataRecord> lookupTableDataRecords = new HashSet<DataRecord>();
        Parser dataParser = new DelimitedDataParser(DATA_FILE_CHARSET);

        try {
            dataParser.init(metadata);
        } catch (ComponentNotReadyException exception) {
            fail("Initialization of the data parser failed!");
        }

        try {
            dataParser.setDataSource(FileUtils.getReadableChannel(null, DATA_FILE_URL));

            DataRecord dataRecord = dataParser.getNext();

            while (dataRecord != null) {
                lookupTableDataRecords.add(dataRecord);
                dataRecord = dataParser.getNext();
            }
        } catch (IOException exception) {
            fail("Opening of the data file failed!");
        } catch (ComponentNotReadyException exception) {
            fail("Populating of the lookup table failed!");
        } catch (JetelException exception) {
            fail("Populating of the lookup table failed!");
        } finally {
            dataParser.close();
        }

        //
        // count the data records and check their content
        //

        int dataRecordsCount = lookupTableDataRecords.size();

        for (DataRecord dataRecord : aspellLookupTable) {
            lookupTableDataRecords.remove(dataRecord);
            dataRecordsCount--;
        }

        assertEquals("The iterator returned invalid number of data records!", 0, dataRecordsCount);
        assertTrue("The iterator did not return all the data records!", lookupTableDataRecords.isEmpty());
    }

    public void testReset() {
        testInit();

        aspellLookupTable.setDataFileCharset(DATA_FILE_CHARSET_INVALID);
        aspellLookupTable.setSpellingThreshold(SPELLING_THRESHOLD_INVALID);

        try {
            aspellLookupTable.reset();
        } catch (ComponentNotReadyException exception) {
            fail("Reseting of the lookup table failed!");
        }

        ConfigurationStatus configurationStatus = new ConfigurationStatus();

        aspellLookupTable.checkConfig(configurationStatus);
        assertTrue("A configuration problem occured even though the reset() method was called!",
                configurationStatus.isEmpty());
    }

}
