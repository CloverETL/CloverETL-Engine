/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (c) Opensys TM by Javlin, a.s. (www.opensys.com)
 *   
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *   
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU   
 *    Lesser General Public License for more details.
 *   
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 *
 */
package org.jetel.util.key;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jetel.data.Defaults;
import org.jetel.exception.JetelException;

/**
 * String based representation of a record key.
 * Result of tokenization at {@link KeyTokenizer}.
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5.10.2009
 */
public class RecordKeyTokens implements Iterable<KeyFieldTokens> {

	private List<KeyFieldTokens> keyFields;
	
	RecordKeyTokens() {
		keyFields = new ArrayList<KeyFieldTokens>();
	}
	
	private void addKeyField(KeyFieldTokens keyField) {
		keyFields.add(keyField);
	}
	
	/**
	 * @param index
	 * @return key field tokens at the given index
	 */
	public KeyFieldTokens getKeyField(int index) {
		return keyFields.get(index);
	}

	/**
	 * @return array with names of all key fields
	 */
	public String[] getKeyFieldNames() {
		String[] result = new String[keyFields.size()];
		
		for (int i = 0; i < keyFields.size(); i++) {
			result[i] = getKeyField(i).getFieldName();
		}
		
		return result;
	}

	/**
	 * @return number of key fields
	 */
	public int size() {
		return keyFields.size();
	}
	
	/**
	 * @param keyRecordString
	 * @return
	 * @throws JetelException 
	 */
	static RecordKeyTokens parseKeyRecord(String keyRecordString) throws JetelException {
		RecordKeyTokens keyRecord = new RecordKeyTokens();

		String[] keyFieldStrings = keyRecordString.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		
		for (int i = 0; i < keyFieldStrings.length; i++) {
			keyRecord.addKeyField(KeyFieldTokens.parseKeyField(keyFieldStrings[i]));
		}
		  
		return keyRecord;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		
		for (KeyFieldTokens keyField : keyFields) {
			result.append(keyField.toString());
			result.append(Defaults.Component.KEY_FIELDS_DELIMITER);
		}
		if (result.length() > 0) {
			result.setLength(result.length() - Defaults.Component.KEY_FIELDS_DELIMITER.length());
		}
		
		return result.toString();
	}

	/* (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	public Iterator<KeyFieldTokens> iterator() {
		return keyFields.iterator();
	}

}
