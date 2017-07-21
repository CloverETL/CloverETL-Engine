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
package org.jetel.data.lookup;

import org.jetel.data.DataRecord;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.NotInitializedException;
import org.jetel.graph.IGraphElement;
import org.jetel.metadata.DataRecordMetadata;

/**
 * <p>The interface of a lookup table specifying the minimum required functionality.</p>
 * <p>The intended use of a lookup table is the following:</p>
 * <ol>
 *   <li>create/obtain an instance of the <code>LookupTable</code></li>
 *   <li>call the {@link IGraphElement#init()} method to populate the lookup table and prepare it for use</li>
 *   <li>use the {@link #put(DataRecord)} and {@link #remove(DataRecord)} methods to manage the lookup data records</li>
 *   <li>use the {@link #createLookup(RecordKey)} or {@link #createLookup(RecordKey, DataRecord)} for lookup</li>
 *   <li>call the {@link IGraphElement#free()} method to free the resources used by the lookup table</li>
 * </ol>
 *
 * @author David Pavlis, Javlin a.s. &lt;david.pavlis@javlin.eu&gt;
 * @author Agata Vackova, Javlin a.s. &lt;agata.vackova@javlin.eu&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 5th February 2009
 * @see Lookup
 * @since 8th July 2004
 */
public interface LookupTable extends IGraphElement, Iterable<DataRecord> {

    public static final String XML_METADATA_ID = "metadata";
    public static final String XML_DBCONNECTION = "dbConnection";

    /**
     * <p>Determines whether the lookup table supports the {@link #put(DataRecord)} method.</p>
     *
     * @return <code>true</code> if the method is supported, <code>false</code> otherwise
     *
     * @since 10th November 2008
     */
    public boolean isPutSupported();

    /**
     * <p>Determines whether the lookup table supports the {@link #remove(DataRecord)}
     * and {@link #remove(HashKey)} methods.</p>
     *
     * @return <code>true</code> if the methods are supported, <code>false</code> otherwise
     *
     * @since 10th November 2008
     */
    public boolean isRemoveSupported();

    /**
     * <p>Returns the metadata associated with the data records stored in this lookup table.</p>
     *
     * @return an instance of the <code>DataRecordMetadata</code> class
     */
    public DataRecordMetadata getMetadata();

    /**
     * <p>Returns metadata used to create the lookup proxy object by calling any of the
     * {@link #createLookup(RecordKey)} or {@link #createLookup(RecordKey, DataRecord)} methods.</p>
     *
     * @return metadata that is used for joining records
     *
     * @throws UnsupportedOperationException if the key cannot be obtained
     * @throws NotInitializedException if the lookup table has not yet been initialized
     * @throws ComponentNotReadyException if the lookup table is not properly configured
     * @throws RuntimeException if metadata is not set by user and can't be obtained for external source (eg. database)
     *
     * @since 7th November 2008
     */
    public DataRecordMetadata getKeyMetadata() throws ComponentNotReadyException;

    /**
     * <p>Puts the given data record into the lookup table. This method will work properly iff
     * the {@link #isPutSupported()} method returns <code>false</code>.</p>
     *
     * @param dataRecord the data record to be put into the lookup table
     *
     * @return <code>true</code> on success, <code>false</code> otherwise
     *
     * @throws UnsupportedOperationException if this method is not supported
     * @throws NotInitializedException if the lookup table has not yet been initialized
     * @throws NullPointerException if the given data record is <code>null</code>
     * @throws IllegalArgumentException if the given data record is not compatible with the lookup table metadata
     *
     * @since 23rd October 2008
     */
    public boolean put(DataRecord dataRecord);

//    public boolean put(RecordKey key, DataRecord recordKey, DataRecord dataRecord);

    /**
     * <p>Removes the given data record from the lookup table. This method will work properly iff
     * the {@link #isPutSupported()} method returns <code>false</code>.</p>
     *
     * @param dataRecord the data record to be removed from the lookup table
     *
     * @return <code>true</code> on success, <code>false</code> otherwise
     *
     * @throws UnsupportedOperationException if this method is not supported
     * @throws NotInitializedException if the lookup table has not yet been initialized
     * @throws NullPointerException if the given data record is <code>null</code>
     * @throws IllegalArgumentException if the given data record is not compatible with the lookup table metadata
     *
     * @since 23rd October 2008
     */
    public boolean remove(DataRecord dataRecord); 

    /**
     * <p>Removes records from the lookup table stored with given key. This method will work properly iff
     * the {@link #isPutSupported()} method returns <code>false</code>.</p>
     *
     * @param key the hash key to be removed from the lookup table
     *
     * @return <code>true</code> on success, <code>false</code> otherwise
     *
     * @throws UnsupportedOperationException if this method is not supported
     * @throws NotInitializedException if the lookup table has not yet been initialized
     * @throws NullPointerException if the given hash key is <code>null</code>
     *
     * @since 23rd October 2008
     */
    public boolean remove(HashKey key); 
    
    /**
     * <p>Attempts to free memory by erasing internal caches of the lookup table.
     * 
     * <p>Behavior in different LookupTable implementations can differ and can give different results.
     * Clearing the caches may result in LookupTable having no entries, for other
     * implementations it might only result in higher memory available after the
     * LookupTable is cleared, and worse performance once the LookupTable is used again.
     */
    public void clear();

//    public boolean removeKey(RecordKey recKey, DataRecord record);

    /**
     * <p>Creates a lookup proxy object for the given lookup key. Returned proxy object can be used to retrieve data
     * records associated with the given lookup key via the <code>Iterator&lt;DataRecord&gt;</code> interface. Lookup
     * proxy object can also be used for continuous searching based on the same key and different data records.</p>
     *
     * @param lookupKey a record key that will be used for lookup
     *
     * @returns a lookup proxy object that can be used for lookup queries with the given lookup key
     *
     * @throws NotInitializedException if the lookup table has not yet been initialized
     * @throws NullPointerException if the given lookup key is <code>null</code>
     * @throws IllegalArgumentException if the given lookup key is not compatible with this lookup table
     * @throws ComponentNotReadyException if the lookup table is not properly configured
     *
     * @see Lookup
     * @since 29th October 2008
     */
    public Lookup createLookup(RecordKey lookupKey) throws ComponentNotReadyException;

    /**
     * <p>Creates a lookup proxy object for the given lookup key. Returned proxy object can be used to retrieve data
     * records associated with the given lookup key via the <code>Iterator&lt;DataRecord&gt;</code> interface. Lookup
     * proxy object can also be used for continuous searching based on the same key and different data records.</p>
     * <p>A lookup table implementation can take advantage of the second parameter to prepare a lookup query that
     * can be reused by simple refilling the specified instance of the <code>DataRecord</code> class. 
     *
     * @param lookupKey a record key that will be used for lookup
     * @param dataRecord a data record that will be used for future lookup queries
     *
     * @returns a lookup proxy object that can be used for lookup queries with the given lookup key and data record
     *
     * @throws NotInitializedException if the lookup table has not yet been initialized
     * @throws NullPointerException if the given lookup key or data record is <code>null</code>
     * @throws IllegalArgumentException if the given lookup key is not compatible with this lookup table
     * @throws ComponentNotReadyException if the lookup table is not properly configured
     *
     * @see Lookup
     * @since 29th October 2008
     */
    public Lookup createLookup(RecordKey lookupKey, DataRecord dataRecord) throws ComponentNotReadyException;

    /**
     * Setter for the current phase when graph is running;
     * @param phase
     */
    public void setCurrentPhase(int phase);
}
