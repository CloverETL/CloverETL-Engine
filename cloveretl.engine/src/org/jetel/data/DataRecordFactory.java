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
package org.jetel.data;

import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.CloverBuffer;

/**
 * Class for factorisation of {@link DataRecord} object. Constructors of {@link DataRecord} are deprecated.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27 Apr 2012
 */
public final class DataRecordFactory {

    /**
	 * Creates new instance of DataRecord based on specified metadata (
	 * how many fields, what field types, etc.)
	 * 
	 * @param _metadata  description of the record structure
     */
    @SuppressWarnings("deprecation")
	public static DataRecord newRecord(DataRecordMetadata metadata) {
    	if (metadata.getNature() == DataRecordNature.TOKEN) {
			return new Token(metadata);
		} else {
			return new DataRecord(metadata);
		}
    }

    /**
     * Creates new instance of Token based on specified metadata.
     */
    public static Token newToken(DataRecordMetadata metadata) {
		Token token = new Token(metadata);
		token.init();
		token.reset();
		return token;
    }
    
    /**
     * Creates dummy token without fields. Can be used for token tracking purpose.
     * Sometimes can be requested to log a token message and no token is available
     * at the time. So this token can be used, initialised an unify later with
     * a real token. 
     * @param name name of artificial empty metadata which will be used for resulted token
     * @return empty token
     */
    public static Token newToken(String name) {
    	return newToken(createEmptyMetadata(name));
    }
    
    private static DataRecordMetadata createEmptyMetadata(String name) {
    	DataRecordMetadata result = new DataRecordMetadata(name);
    	return result;
    }
    
    /**
     * @return direct {@link CloverBuffer} suitable to handle a serialised data record
     */
    public static CloverBuffer newRecordCloverBuffer() {
    	return CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
    }
    
}
