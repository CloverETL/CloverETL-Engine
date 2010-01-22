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

import org.jetel.exception.JetelException;
import org.jetel.util.string.StringUtils;

/**
 * Collection of static methods for manipulating and creating tokens witch represent a record key.
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5.10.2009
 */
public class KeyTokenizer {

	/**
	 * Tokenizes the given raw form of a record key.
	 * @param recordKeyString
	 * @return
	 * @throws JetelException
	 */
	public static RecordKeyTokens tokenizeRecordKey(String recordKeyString) throws JetelException {
		if (StringUtils.isEmpty(recordKeyString)) {
			throw new IllegalArgumentException("RecordKeyString cannot be empty.");
		}
		RecordKeyTokens keyRecord = RecordKeyTokens.parseKeyRecord(recordKeyString);
		
		return keyRecord;
	}
	
	public static RecordKeyTokens createEmptyRecordKey() {
		return new RecordKeyTokens();
	}
}
