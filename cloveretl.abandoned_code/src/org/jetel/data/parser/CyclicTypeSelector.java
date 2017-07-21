/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
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
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.data.parser;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.util.Properties;

import org.jetel.data.DataRecord;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Simple type-selector implementation. It is supposed to be used for
 * cyclic multi-level input files. An aray of record types is specified in ctor.
 * Input file starts with record of first type, follows with second type, ... First type
 * follows again after last type.
 * 
 * @since 15/12/2006
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 */
public class CyclicTypeSelector implements TypeSelector {
	
	private int typeIdx;
	private int typeCnt;

	public CyclicTypeSelector() {
	}

	public int init(DataRecordMetadata[] metadata, Properties properties) {
		typeCnt = metadata.length;
		typeIdx = 0;
		return 0;
	}

	public int choose(ByteBuffer nextData, CharsetDecoder decoder) {
		return typeIdx;
	}

	public void presentRecord(DataRecord record) {
		typeIdx = (typeIdx + 1)%typeCnt;
	}

	public void finish() {
	}

	public int choose(CharBuffer nextData, CharsetDecoder decoder) throws JetelException {
		// TODO Auto-generated method stub
		return 0;
	}

	public int choose(CharBuffer nextData) throws JetelException {
		// TODO Auto-generated method stub
		return 0;
	}

	public int lookAheadCharacters() {
		// TODO Auto-generated method stub
		return 0;
	}

}
