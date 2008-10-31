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
package org.jetel.data.lookup;

import org.jetel.data.DataRecord;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
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
 * @author David Pavlis <david.pavlis@javlin.cz>
 * @author Martin Janik <martin.janik@javlin.cz>
 *
 * @version 31st October 2008
 * @see Lookup
 * @since 8th July 2004
 */
public interface LookupTable extends IGraphElement, Iterable<DataRecord> {

    public static final String XML_METADATA_ID = "metadata";
    public static final String XML_DBCONNECTION = "dbConnection";

    /**
     * <p>Determines whether the lookup table is read-only or not. Read-only lookup tables DO NOT support
     * the {@link #put(Object, DataRecord)} and {@link #remove(Object)} methods.</p>
     *
     * @return <code>true</code> if the lookup table is read-only, <code>false</code> otherwise
     *
     * @since 23rd October 2008
     */
    public boolean isReadOnly();

    /**
     * <p>Returns the meta data associated with the data records stored in this lookup table.</p>
     *
     * @return an instance of the <code>DataRecordMetadata</code> class
     */
    public DataRecordMetadata getMetadata();

    /**
     * <p>Puts the given data record into the lookup table. This method will work properly iff
     * the {@link #isReadOnly()} method returns <code>false</code>.</p>
     *
     * @param dataRecord the data record to be put into the lookup table
     *
     * @return <code>true</code> on success, <code>false</code> otherwise
     *
     * @throws UnsupportedOperationException if this method is not supported
     * @throws NotInitializedException if the lookup table has not yet been initialized
     * @throws NullPointerException if the given data record is <code>null</code>
     * @throws IllegalArgumentException if the given data record is not compatible with the lookup table meta data
     *
     * @since 23rd October 2008
     */
    public boolean put(DataRecord dataRecord);

    /**
     * <p>Removes the given data record from the lookup table. This method will work properly iff
     * the {@link #isReadOnly()} method returns <code>false</code>.</p>
     *
     * @param dataRecord the data record to be put into the lookup table
     *
     * @return <code>true</code> on success, <code>false</code> otherwise
     *
     * @throws UnsupportedOperationException if this method is not supported
     * @throws NotInitializedException if the lookup table has not yet been initialized
     * @throws NullPointerException if the given data record is <code>null</code>
     * @throws IllegalArgumentException if the given data record is not compatible with the lookup table meta data
     *
     * @since 23rd October 2008
     */
    public boolean remove(DataRecord dataRecord); 

    /**
     * <p>Performs lookup using the given lookup key and data record and returns the first matching data record.
     * To retrieve all the matching data records, use the {@link #createLookup(RecordKey)} method.</p>
     *
     * @param lookuKey the lookup key used for lookup
     * @param dataRecord the data record to be used for lookup
     *
     * @return a <code>DataRecord</code> or <code>null</code> if no data record is stored under the lookup key
     *
     * @throws NotInitializedException if the lookup table has not yet been initialized
     * @throws NullPointerException if the given lookup key or data record is <code>null</code>
     * @throws IllegalArgumentException if the given lookup key is not compatible with this lookup table
     *
     * @since 23rd October 2008
     */
    @Deprecated
    public DataRecord get(RecordKey lookuKey, DataRecord dataRecord);

    /**
     * <p>Returns the next data record stored under the same key as the previous one successfully retrieved
     * by calling the {@link #get(HashKey)} method.</p>
     * <p>This method MAY NOT work properly when called from multiple threads! Use the {@link #createLookup(RecordKey)}
     * method instead of this one.</p>
     *
     * @return a <code>DataRecord</code> or <code>null</code> if no other data record is stored under the same key
     *
     * @throws NotInitializedException if the lookup table has not yet been initialized
     * @throws IllegalStateException if the {@link #get(HashKey)} method has not yet been called
     *
     * @deprecated
     */
    @Deprecated
    public DataRecord getNext();

    /**
     * <p>Returns the number of data records found by the last call to the {@link #get(HashKey)} method.</p>
     * <p>This method MAY NOT work properly when called from multiple threads! Use the {@link #createLookup(RecordKey)}
     * method instead of this one.</p>
     *
     * @return the number of matching data records or <code>-1</code> if this operation cannot be applied to this
     * lookup table implementation
     *
     * @throws NotInitializedException if the lookup table has not yet been initialized
     * @throws IllegalStateException if the {@link #get(HashKey)} method has not yet been called
     *
     * @deprecated
     */
    @Deprecated
    public int getNumFound();

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
     *
     * @see Lookup
     * @since 29th October 2008
     */
    public Lookup createLookup(RecordKey lookupKey);

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
     *
     * @see Lookup
     * @since 29th October 2008
     */
    public Lookup createLookup(RecordKey lookupKey, DataRecord dataRecord);

}
