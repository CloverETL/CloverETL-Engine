/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.data;

import java.util.*;
import java.io.*;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  Simple lookup table which reads data from flat file and creates Map structure.
 *
 * @author     dpavlis
 * @since    May 2, 2002
 */
public class SimpleLookupTable {

	private DataRecordMetadata metadata;
	private DataParser dataParser;
	private Map lookupTable;
	private InputStream inData;
	private RecordKey key;
	
	/**
	* Default capacity of HashMap when standard constructor is used.
	*/
	private final static int DEFAULT_INITIAL_CAPACITY = 512;


	/**
	 *Constructor for the SimpleLookupTable object.<br>It uses HashMap class to
	 *store key->data pairs in it.
	 *
	 * @param  parser    Reference to parser which should be used for parsing input data
	 * @param  metadata  Metadata describing input data
	 * @param  in        InputStream of data
	 * @param  keys      Names of fields which comprise key to lookup table
	 * @since            May 2, 2002
	 */
	public SimpleLookupTable(DataRecordMetadata metadata, String[] keys, DataParser parser, InputStream in) {
		this.dataParser = parser;
		this.metadata = metadata;
		inData = in;
		lookupTable = new HashMap(DEFAULT_INITIAL_CAPACITY);
		key = new RecordKey(keys, metadata);
	}


	/**
	 *Constructor for the SimpleLookupTable object.
	 *
	 * @param  parser     Reference to parser which should be used for parsing input data
	 * @param  metadata   Metadata describing input data
	 * @param  in         InputStream of data
	 * @param  keys       Names of fields which comprise key to lookup table
	 * @param  mapObject  Object implementing Map interface. It will be used to hold key->data pairs
	 * @since             May 2, 2002
	 */
	public SimpleLookupTable(DataRecordMetadata metadata, String[] keys, DataParser parser, InputStream in, Map mapObject) {
		this.dataParser = parser;
		this.metadata = metadata;
		inData = in;
		lookupTable = mapObject;
		key = new RecordKey(keys, metadata);
	}


	/**
	 *  Looks-up data based on speficied key.<br> The key should be result of calling RecordKey.getKeyString()
	 *
	 * @param  keyString  Key String which will be used for lookup of data record
	 * @return            Associated DataRecord or NULL if not found
	 * @since             May 2, 2002
	 */
	public DataRecord get(String keyString) {
		return (DataRecord) lookupTable.get(keyString);
	}


	/**
	 *  Initializtaion of lookup table - loading all data into it.
	 *
	 * @exception  IOException  Description of Exception
	 * @since                   May 2, 2002
	 */
	public void init() throws IOException {
		DataRecord record;
		key.init();
		dataParser.open(inData, metadata);
		// populate the lookupTable (Map) with data
		while ((record = dataParser.getNext()) != null) {
			lookupTable.put(key.getKeyString(record), record);
		}
		dataParser.close();
	}

}

