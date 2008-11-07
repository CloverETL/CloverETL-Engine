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

import com.swabunga.spell.engine.Configuration;
import com.swabunga.spell.engine.PropertyConfiguration;
import com.swabunga.spell.engine.SpellDictionary;
import com.swabunga.spell.engine.SpellDictionaryHashMap;
import com.swabunga.spell.engine.Word;

import java.io.IOException;

import java.nio.channels.Channel;
import java.nio.charset.Charset;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.parser.DataParser;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.data.parser.FixLenCharDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.InvalidGraphObjectNameException;
import org.jetel.exception.JetelException;
import org.jetel.exception.NotInitializedException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;

import org.w3c.dom.Element;

/**
 * Represents an Aspell-like lookup table. This lookup table returns data records with matching or similar lookup keys
 * as the specified lookup key. This may be useful e. g. when looking for a street the name of which is misspelled.
 *
 * @author Martin Janik <martin.janik@javlin.cz>
 *
 * @version 3rd November 2008
 * @since 27th October 2008
 */
public final class AspellLookupTable extends GraphElement implements LookupTable {

    /**
     * Represents a Jazzy configuration class required for Jazzy configuration tools to work correctly.
     * Without this class, Jazzy's properties cannot be modified due to an internal bug.
     *
     * @author Martin Janik <martin.janik@javlin.cz>
     *
     * @version 27th October 2008
     * @since 27th October 2008
     */
    public static final class JazzyConfiguration extends Configuration {

        /** the configuration properties used by Jazzy */
        private static final Properties configurationProperties = new PropertyConfiguration().prop;

        @Override
        public void setBoolean(String key, boolean value) {
            configurationProperties.put(key, Boolean.toString(value));
        }

        @Override
        public boolean getBoolean(String key) {
            return Boolean.parseBoolean(configurationProperties.getProperty(key));
        }

        @Override
        public void setInteger(String key, int value) {
            configurationProperties.put(key, Integer.toString(value));
        }

        @Override
        public int getInteger(String key) {
            return Integer.parseInt(configurationProperties.getProperty(key));
        }

    }

    /**
     * An implementation of the lookup proxy class for the Aspell lookup table.
     *
     * @author Martin Janik <martin.janik@javlin.cz>
     *
     * @version 31st October 2008
     * @since 31st October 2008
     */
    private final class AspellLookup implements Lookup {

        /** the record key used for lookup */
        private final RecordKey lookupKey;

        /** the data record used for lookup */
        private DataRecord dataRecord;

        /** the queue of matching data records returned by the last lookup */
        private Queue<DataRecord> matchingDataRecords = null;
        /** the number of data records returned by the last lookup */
        private int matchingDataRecordsCount = -1;

        /**
         * Creates an instance of the <code>AspellLookup</code> class for the given lookup key.
         *
         * @param lookupKey the lookup key that will be used for lookup
         */
        public AspellLookup(RecordKey lookupKey, DataRecord dataRecord) {
            this.lookupKey = lookupKey;
            this.dataRecord = dataRecord;
        }

        public RecordKey getKey() {
            return lookupKey;
        }

        public LookupTable getLookupTable() {
            return AspellLookupTable.this;
        }

        public void seek() {
            if (dataRecord == null) {
                throw new IllegalStateException("Data record not set, use the seek(DataRecord) method!");
            }

            matchingDataRecords = performLookup(lookupKey, dataRecord);
            matchingDataRecordsCount = matchingDataRecords.size();
        }

        public void seek(DataRecord dataRecord) {
            if (dataRecord == null) {
                throw new NullPointerException("dataRecord");
            }

            this.dataRecord = dataRecord;

            matchingDataRecords = performLookup(lookupKey, dataRecord);
            matchingDataRecordsCount = matchingDataRecords.size();
        }

        public int getNumFound() {
            if (matchingDataRecords == null) {
                throw new IllegalStateException("The seek() method has NOT been called!");
            }

            return matchingDataRecordsCount;
        }

        public boolean hasNext() {
            if (matchingDataRecords == null) {
                throw new IllegalStateException("The seek() method has NOT been called!");
            }

            return !matchingDataRecords.isEmpty();
        }

        public DataRecord next() {
            if (matchingDataRecords == null) {
                throw new IllegalStateException("The seek() method has NOT been called!");
            }

            return matchingDataRecords.remove();
        }

        public void remove() {
            throw new UnsupportedOperationException("Method not supported!");
        }

    }

    /**
     * The iterator used for iteration over all the data records stored in the lookup table.
     *
     * @author Martin Janik <martin.janik@javlin.cz>
     *
     * @version 27th October 2008
     * @since 27th October 2008
     */
    private final class AspellLookupTableIterator implements Iterator<DataRecord> {

        /** iterator over sets of similar data records associated with the same lookup key */
        private Iterator<Set<DataRecord>> similarDataRecordsIterator;
        /** iterator over similar data records associated with the same lookup key */
        private Iterator<DataRecord> dataRecordsIterator = null;

        /**
         * Creates an instance of the <code>AspellLookupTableIterator</code> class that iterates over the data
         * records stored in the lookup table of the outer class.
         */
        public AspellLookupTableIterator() {
            this.similarDataRecordsIterator = dataRecords.values().iterator();

            if (similarDataRecordsIterator.hasNext()) {
                this.dataRecordsIterator = similarDataRecordsIterator.next().iterator();
            }
        }

        public boolean hasNext() {
            if (dataRecordsIterator == null) {
                return false;
            }

            return (dataRecordsIterator.hasNext() || similarDataRecordsIterator.hasNext());
        }

        public DataRecord next() {
            if (dataRecordsIterator == null) {
                throw new NoSuchElementException();
            }

            if (!dataRecordsIterator.hasNext()) {
                dataRecordsIterator = similarDataRecordsIterator.next().iterator();
            }

            return dataRecordsIterator.next();
        }

        public void remove() {
            throw new UnsupportedOperationException("Method not supported!");
        }

    }

    /** the type of the lookup table */
    private static final String LOOKUP_TABLE_TYPE = "aspellLookup";

    /** the required XML attribute used to store the lookup key field used for data record lookup */
    private static final String XML_LOOKUP_KEY_FIELD_ATTRIBUTE = "lookupKeyField";
    /** the required XML attribute used to store the data file URL */
    private static final String XML_DATA_FILE_URL_ATTRIBUTE = "dataFileUrl";
    /** the optional XML attribute used to store the data file character set */
    private static final String XML_DATA_FILE_CHARSET_ATTRIBUTE = "dataFileCharset";
    /** the optional XML attribute used to store the spelling threshold */
    private static final String XML_SPELLING_THRESHOLD_ATTRIBUTE = "spellingThreshold";

    /** the default data file character set */
    private static final String DEFAULT_DATA_FILE_CHARSET = Defaults.DataParser.DEFAULT_CHARSET_DECODER;
    /** the default spelling threshold */
    private static final int DEFAULT_SPELLING_THRESHOLD = 230;

    static {
        // this system property needs to be set to allow modification of the Jazzy's SPELL_THRESHOLD property
        System.setProperty("jazzy.config", "org.jetel.lookup.AspellLookupTable$JazzyConfiguration");
    }

    /**
     * Creates an instance of the <code>AspellLookupTable</code> class from an XML element.
     *
     * @param graph the transformation graph the lookup table belongs to
     * @param xmlElement the XML element that should be used for construction
     *
     * @return an instance of the <code>AspellLookupTable</code> class
     *
     * @throws XMLConfigurationException when a required attribute is missing
     */
    public static AspellLookupTable fromXML(TransformationGraph graph, Element xmlElement)
            throws XMLConfigurationException {
        AspellLookupTable aspellLookupTable = null;

        ComponentXMLAttributes lookupTableAttributes = new ComponentXMLAttributes(xmlElement, graph);
        String type = null;

        try {
            type = lookupTableAttributes.getString(XML_TYPE_ATTRIBUTE);
        } catch (Exception exception) {
            throw new XMLConfigurationException("The " + StringUtils.quote(XML_TYPE_ATTRIBUTE)
                    + " attribute is missing!", exception);
        }

        if (!type.equalsIgnoreCase(LOOKUP_TABLE_TYPE)) {
            throw new XMLConfigurationException("The " + StringUtils.quote(XML_TYPE_ATTRIBUTE)
                    + " attribute contains a value NOT compatible with this lookup table!");
        }

        try {
            aspellLookupTable = new AspellLookupTable(
                    lookupTableAttributes.getString(XML_ID_ATTRIBUTE),
                    graph.getDataRecordMetadata(lookupTableAttributes.getString(XML_METADATA_ID)),
                    lookupTableAttributes.getString(XML_LOOKUP_KEY_FIELD_ATTRIBUTE),
                    lookupTableAttributes.getString(XML_DATA_FILE_URL_ATTRIBUTE));
            aspellLookupTable.setGraph(graph);

            if (lookupTableAttributes.exists(XML_NAME_ATTRIBUTE)) {
                aspellLookupTable.setName(lookupTableAttributes.getString(XML_NAME_ATTRIBUTE));
            }

            if (lookupTableAttributes.exists(XML_DATA_FILE_CHARSET_ATTRIBUTE)) {
                aspellLookupTable.setDataFileCharset(lookupTableAttributes.getString(XML_DATA_FILE_CHARSET_ATTRIBUTE));
            }

            if (lookupTableAttributes.exists(XML_SPELLING_THRESHOLD_ATTRIBUTE)) {
                aspellLookupTable.setSpellingThreshold(lookupTableAttributes.getInteger(XML_SPELLING_THRESHOLD_ATTRIBUTE));
            }
        } catch (AttributeNotFoundException exception) {
            throw new XMLConfigurationException("Missing a required attribute!", exception);
        } catch (Exception exception) {
            throw new XMLConfigurationException("Error creating the lookup table!", exception);
        }

        return aspellLookupTable;
    }

    /** the data record meta data associated with this lookup table */
    private final DataRecordMetadata metadata;
    /** the lookup key field used for populating the lookup table and data records lookup */
    private final String lookupKeyField;
    /** the URL of a data file used to populate the lookup table */
    private final String dataFileUrl;

    /** the character set of data stored in the data file */
    private String dataFileCharset;

    /** the spell dictionary used for data lookup */
    private SpellDictionary dictionary = null;
    /** the map containing all the data records stored in the lookup table */
    private Map<String, Set<DataRecord>> dataRecords = null;

    /** the queue of matching data records returned by the last lookup */
    private Queue<DataRecord> matchingDataRecords = null;
    /** the number of data records returned by the last lookup */
    private int matchingDataRecordsCount = -1;

    /**
     * Creates a new instance of the <code>AspellLookupTable</code> class. Compulsory attributes are set
     * to values provided during construction, optional attributes are set to their default values.
     *
     * @param id an identification of the lookup table
     * @param metadata the data record meta data associated with this lookup table
     * @param lookupKeyField the lookup key field used for populating the lookup table and data records lookup
     * @param dataFileUrl the URL of a data file used to populate the lookup table
     *
     * @throws InvalidGraphObjectNameException if the id is invalid
     */
    public AspellLookupTable(String id, DataRecordMetadata metadata, String lookupKeyField, String dataFileUrl) {
        super(id);

        this.metadata = metadata;
        this.lookupKeyField = lookupKeyField;
        this.dataFileUrl = dataFileUrl;

        setDataFileCharset(DEFAULT_DATA_FILE_CHARSET);
        setSpellingThreshold(DEFAULT_SPELLING_THRESHOLD);
    }

    public boolean isReadOnly() {
        return true;
    }

    public DataRecordMetadata getMetadata() {
        return metadata;
    }

    /**
     * Sets the character set used for data records stored in the lookup table data file.
     *
     * @param dataFileCharset a character set to be used
     */
    public void setDataFileCharset(String dataFileCharset) {
        this.dataFileCharset = dataFileCharset;
    }

    /**
     * Sets the spelling threshold used by Jazzy when looking for words. The higher the value, the more
     * tolerant Jazzy is to spelling mistakes. The spelling threshold is shared by all instances of the
     * <code>AspellLookupTable</code> class and should be set to a value greater than zero.
     *
     * @param spellingThreshold the spelling threshold to be set
     */
    public void setSpellingThreshold(int spellingThreshold) {
        Configuration.getConfiguration().setInteger(Configuration.SPELL_THRESHOLD, spellingThreshold);
    }

    /**
     * Returns the spelling threshold currently used by Jazzy.
     *
     * @return an <code>int<code> value specifying the spelling threshold
     */
    public int getSpellingThreshold() {
        return Configuration.getConfiguration().getInteger(Configuration.SPELL_THRESHOLD);
    }

    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);

        if (metadata == null) {
            status.add(new ConfigurationProblem("The meta data is NULL!",
                    Severity.ERROR, this, Priority.HIGH, "metadata"));
        } else {
            if (metadata.getRecType() != DataRecordMetadata.DELIMITED_RECORD
                    && metadata.getRecType() != DataRecordMetadata.FIXEDLEN_RECORD
                    && metadata.getRecType() != DataRecordMetadata.MIXED_RECORD) {
                status.add(new ConfigurationProblem("The meta data record type is not supported!",
                        Severity.ERROR, this, Priority.HIGH, "metadata"));
            }

            if (lookupKeyField == null) {
                status.add(new ConfigurationProblem("The lookup key field is NULL!",
                        Severity.ERROR, this, Priority.HIGH, "lookupKeyField"));
            }

            if (metadata.getField(lookupKeyField) == null) {
                status.add(new ConfigurationProblem("The lookup key field is NOT valid!",
                        Severity.ERROR, this, Priority.HIGH, "lookupKeyField"));
            } else if (metadata.getField(lookupKeyField).getType() != DataFieldMetadata.STRING_FIELD) {
                status.add(new ConfigurationProblem("The lookup key field is not a string!",
                        Severity.ERROR, this, Priority.HIGH, "lookupKeyField"));
            }
        }

        if (dataFileUrl == null) {
            status.add(new ConfigurationProblem("The data file URL is NULL!",
                    Severity.ERROR, this, Priority.HIGH, "dataFileUrl"));
        } else {
            Channel dataFileChannel = null;

            try {
                dataFileChannel = FileUtils.getReadableChannel(
                        (getGraph() != null) ? getGraph().getProjectURL() : null, dataFileUrl);
            } catch (IOException exception) {
                status.add(new ConfigurationProblem("The data file URL is NOT valid!",
                        Severity.ERROR, this, Priority.NORMAL, "dataFileUrl"));
            } finally {
                if (dataFileChannel != null) {
                    try {
                        dataFileChannel.close();
                    } catch (IOException exception) {
                        // OK, don't do anything
                    }
                }
            }
        }

        if (!Charset.isSupported(dataFileCharset)) {
            status.add(new ConfigurationProblem("The data file charset is not supported!",
                    Severity.ERROR, this, Priority.NORMAL, "dataFileCharset"));
        }

        if (getSpellingThreshold() <= 0) {
            status.add(new ConfigurationProblem("The spelling threshold is less than one!",
                    Severity.WARNING, this, Priority.NORMAL, "spellingThreshold"));
        }

        return status;
    }

    @Override
    public synchronized void init() throws ComponentNotReadyException {
        if (isInitialized()) {
            throw new IllegalStateException("The lookup table has already been initialized!");
        }

        super.init();

        try {
            dictionary = new SpellDictionaryHashMap();
        } catch (IOException exception) {
            throw new ComponentNotReadyException("Error creating the dictionary!", exception);
        }

        dataRecords = new HashMap<String, Set<DataRecord>>();

        Parser dataParser = null;

        if (getMetadata().getRecType() == DataRecordMetadata.DELIMITED_RECORD) {
            dataParser = new DelimitedDataParser(dataFileCharset);
        } else if (getMetadata().getRecType() == DataRecordMetadata.FIXEDLEN_RECORD) {
            dataParser = new FixLenCharDataParser(dataFileCharset);
        } else {
            dataParser = new DataParser(dataFileCharset);
        }

        dataParser.init(getMetadata());

        try {
            dataParser.setDataSource(FileUtils.getReadableChannel(
                    (getGraph() != null) ? getGraph().getProjectURL() : null, dataFileUrl));

            DataRecord dataRecord = dataParser.getNext();

            while (dataRecord != null) {
                String dataRecordKey = dataRecord.getField(lookupKeyField).toString();
                Set<DataRecord> similarDataRecords = dataRecords.get(dataRecordKey);

                if (similarDataRecords == null) {
                    dictionary.addWord(dataRecordKey);

                    similarDataRecords = new HashSet<DataRecord>();
                    dataRecords.put(dataRecordKey, similarDataRecords);
                }

                similarDataRecords.add(dataRecord);

                dataRecord = dataParser.getNext();
            }
        } catch (IOException exception) {
            throw new ComponentNotReadyException("Error opening the data file!", exception);
        } catch (JetelException exception) {
            throw new ComponentNotReadyException("Error populating the lookup table!", exception);
        } finally {
            dataParser.close();
        }
    }

    public boolean put(DataRecord dataRecord) {
        throw new UnsupportedOperationException("Method not supported!");
    }

    public boolean remove(DataRecord dataRecord) {
        throw new UnsupportedOperationException("Method not supported!");
    }

    public boolean remove(HashKey key) {
        throw new UnsupportedOperationException("Method not supported!");
    }

    public synchronized DataRecord get(RecordKey key, DataRecord keyRecord) {
        if (!isInitialized()) {
            throw new NotInitializedException("The lookup table has NOT been initialized!");
        }

        matchingDataRecords = performLookup(key, keyRecord);
        matchingDataRecordsCount = matchingDataRecords.size();

        return matchingDataRecords.poll();
    }

    private synchronized Queue<DataRecord> performLookup(RecordKey lookupKey, DataRecord dataRecord) {
        if (lookupKey == null) {
            throw new NullPointerException("key");
        }

        if (dataRecord == null) {
            throw new NullPointerException("keyRecord");
        }

        if (lookupKey.getLength() != 1) {
            throw new IllegalArgumentException("The lookupKey cannot be composed from multiple fields!");
        }

        DataField lookupKeyField = dataRecord.getField(lookupKey.getKeyFields()[0]);

        if (lookupKeyField.getType() != DataFieldMetadata.STRING_FIELD) {
            throw new IllegalArgumentException("The lookup key field is not a string!");
        }

        Queue<DataRecord> matchingDataRecords = new LinkedList<DataRecord>();

        @SuppressWarnings("unchecked")
        List<Word> suggestions = dictionary.getSuggestions(lookupKeyField.toString(), 0);

        for (Word suggestion : suggestions) {
            matchingDataRecords.addAll(dataRecords.get(suggestion.getWord()));
        }

        return matchingDataRecords;
    }

    @Deprecated
    public synchronized DataRecord getNext() {
        if (!isInitialized()) {
            throw new NotInitializedException("The lookup table has NOT been initialized!");
        }

        if (matchingDataRecords == null) {
            throw new IllegalStateException("The get() method has NOT been called!");
        }

        return matchingDataRecords.poll();
    }

    @Deprecated
    public synchronized int getNumFound() {
        if (!isInitialized()) {
            throw new NotInitializedException("The lookup table has NOT been initialized!");
        }

        if (matchingDataRecords == null) {
            throw new IllegalStateException("The get() method has NOT been called!");
        }

        return matchingDataRecordsCount;
    }

    public Lookup createLookup(RecordKey lookupKey, DataRecord inRecord) {
        if (!isInitialized()) {
            throw new NotInitializedException("The lookup table has NOT been initialized!");
        }

        if (lookupKey == null) {
            throw new NullPointerException("key");
        }

        if (lookupKey.getLength() != 1) {
            throw new IllegalArgumentException("The lookupKey cannot be composed from multiple fields!");
        }

        if (lookupKey.getMetadata().getField(lookupKey.getKeyFields()[0]).getType() != DataFieldMetadata.STRING_FIELD) {
            throw new IllegalArgumentException("The lookup key field is not a string!");
        }

        return new AspellLookup(lookupKey, inRecord);
    }

    public Lookup createLookup(RecordKey lookupKey) {
        return createLookup(lookupKey, null);
    }

    public synchronized Iterator<DataRecord> iterator() {
        if (!isInitialized()) {
            throw new NotInitializedException("The lookup table has NOT been initialized!");
        }

        return new AspellLookupTableIterator();
    }

    @Override
    public synchronized void reset() throws ComponentNotReadyException {
        if (!isInitialized()) {
            throw new NotInitializedException("The lookup table has NOT been initialized!");
        }

        super.reset();

        setDataFileCharset(DEFAULT_DATA_FILE_CHARSET);
        setSpellingThreshold(DEFAULT_SPELLING_THRESHOLD);

        try {
            dictionary = new SpellDictionaryHashMap();
        } catch (IOException exception) {
            throw new ComponentNotReadyException("Error creating the dictionary", exception);
        }

        dataRecords = new HashMap<String, Set<DataRecord>>();

        matchingDataRecords = null;
        matchingDataRecordsCount = -1;
    }

    @Override
    public synchronized void free() {
        if (!isInitialized()) {
            throw new NotInitializedException("The lookup table has NOT been initialized!");
        }

        super.free();

        dictionary = null;
        dataRecords = null;

        matchingDataRecords = null;
        matchingDataRecordsCount = -1;
    }

}
