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
import org.jetel.metadata.DataRecordMetadata;

/**
 *  This class serves the role of DataRecord to recordKey(String value) mapper.
 *
 * @author      dpavlis
 * @since       May 2, 2002
 * @revision    $Revision$
 * @created     January 26, 2003
 */
public class RecordKey {

	private int keyFields[];
	private DataRecordMetadata metadata;
	private String keyFieldNames[];
	private final static char KEY_ITEMS_DELIMITER = ':';
	private final static int DEFAULT_KEY_LENGTH = 64;

	private StringBuffer keyStr;


	/**
	 *  Constructor for the RecordKey object
	 *
	 * @param  keyFieldNames  Description of Parameter
	 * @param  metadata       Description of Parameter
	 * @since                 May 2, 2002
	 */
	public RecordKey(String keyFieldNames[], DataRecordMetadata metadata) {
		this.metadata = metadata;
		this.keyFieldNames = keyFieldNames;
	}

	// end init

	/**
	 *  Assembles String key from current record's fields
	 *
	 * @param  record  Description of Parameter
	 * @return         The KeyString value
	 * @since          May 2, 2002
	 */
	public String getKeyString(DataRecord record) {
		keyStr.setLength(0);
		// clean buffer
		for (int i = 0; i < keyFields.length; i++) {
			keyStr.append(record.getField(keyFields[i]).toString());
			// not used for now keyStr.append(KEY_ITEMS_DELIMITER);
		}
		return keyStr.toString();
	}


	/**
	 *  Performs initialization of internal data structures
	 *
	 * @since    May 2, 2002
	 */
	public void init() {

		Integer position;
		keyStr = new StringBuffer(DEFAULT_KEY_LENGTH);
		keyFields = new int[keyFieldNames.length];
		Map fields = metadata.getFieldNames();

		for (int i = 0; i < keyFieldNames.length; i++) {
			if ((position = (Integer) fields.get(keyFieldNames[i])) != null) {
				keyFields[i] = position.intValue();
			} else {
				throw new RuntimeException("Field name specified as a key doesn't exist: " + keyFieldNames[i]);
			}
		}
	}


	/**
	 *  Gets the keyFields attribute of the RecordKey object
	 *
	 * @return    The keyFields value
	 */
	public int[] getKeyFields() {
		return keyFields;
	}


	// end getKeyString

	/*
	 *  public int compareTo(Object otherKey){
	 *  for (int i = 0; i < keyFields.length; i++) {
	 *  keyStr.append(record.getField(keyFields[i]).toString());
	 *  / not used for now keyStr.append(KEY_ITEMS_DELIMITER);
	 *  }
	 *  }
	 */
	/**
	 *  Compares two records (of the same layout) based on defined key-fields and returns (-1;0;1) if (< ; = ; >)
	 *
	 * @param  record1  Description of the Parameter
	 * @param  record2  Description of the Parameter
	 * @return          -1 ; 0 ; 1
	 */
	public int compare(DataRecord record1, DataRecord record2) {
		int compResult;
		if (record1.getMetadata() != record2.getMetadata()) {
			throw new RuntimeException("Can't compare - records have different metadata associated." +
					" Possibly different structure");
		}
		for (int i = 0; i < keyFields.length; i++) {
			compResult = record1.getField(keyFields[i]).compareTo(record2.getField(keyFields[i]));
			if (compResult != 0) {
				return compResult;
			}
		}
		return 0;
		// seem to be the same
	}


	/**
	 *  Compares two records (can have different layout) based on defined key-fields
	 *  and returns (-1;0;1) if (< ; = ; >).<br>
	 *  The particular fields to be compared have to be of the same type !
	 *
	 * @param  secondKey  RecordKey defined for the second record
	 * @param  record1    First record
	 * @param  record2    Second record
	 * @return            -1 ; 0 ; 1
	 */
	public int compare(RecordKey secondKey, DataRecord record1, DataRecord record2) {
		int compResult;
		int[] record2KeyFields = secondKey.getKeyFields();
		if (keyFields.length != record2KeyFields.length) {
			throw new RuntimeException("Can't compare. keys have different number of DataFields");
		}
		for (int i = 0; i < keyFields.length; i++) {
			compResult = record1.getField(keyFields[i]).compareTo(record2.getField(record2KeyFields[i]));
			if (compResult != 0) {
				return compResult;
			}
		}
		return 0;
		// seem to be the same
	}


	/**
	 *  Description of the Method
	 *
	 * @param  record1  Description of the Parameter
	 * @param  record2  Description of the Parameter
	 * @return          Description of the Return Value
	 */
	public boolean equals(DataRecord record1, DataRecord record2) {
		if (record1.getMetadata() != record2.getMetadata()) {
			throw new RuntimeException("Can't compare - records have different metadata associated." +
					" Possibly different structure");
		}
		for (int i = 0; i < keyFields.length; i++) {
			if (!record1.getField(keyFields[i]).equals(record2.getField(keyFields[i]))) {
				return false;
			}
		}
		return true;
	}

}
// end RecordKey


