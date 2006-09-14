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

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Wrapper for fix-length data parsers working in byte and char mode.
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 *
 */
public class FixLenDataParser3 implements Parser {

	private FixLenByteDataParser byteParser;
	private FixLenCharDataParser charParser;
	private Parser parser;

	public FixLenDataParser3(boolean byteMode) {
		if (byteMode) {
			parser = byteParser = new FixLenByteDataParser(); 
			charParser = null; 
		} else {
			parser = charParser = new FixLenCharDataParser();
			byteParser = null;
		}
	}

	public FixLenDataParser3(String charset, boolean byteMode) {
		if (byteMode) {
			parser = new FixLenByteDataParser(charset); 
			charParser = null; 
		} else {
			parser = charParser = new FixLenCharDataParser(charset); 
		}
	}

	public void close() {
		parser.close();
	}

	public IParserExceptionHandler getExceptionHandler() {
		return parser.getExceptionHandler();
	}

	public DataRecord getNext() throws JetelException {
		return parser.getNext();
	}

	public DataRecord getNext(DataRecord record) throws JetelException {
		return parser.getNext(record);
	}

	public PolicyType getPolicyType() {
		return parser.getPolicyType();
	}

	public void open(Object inputDataSource, DataRecordMetadata _metadata) throws ComponentNotReadyException {
		parser.open(inputDataSource, _metadata);
	}

	public void setExceptionHandler(IParserExceptionHandler handler) {
		parser.setExceptionHandler(handler);
	}

	public int skip(int nRec) throws JetelException {
		return parser.skip(nRec);
	}

	public String getCharsetName() {
		if (charParser == null) {
			return charParser.getCharsetName();
		} else {
			return byteParser.getCharsetName();
		}
	}

	public boolean isEnableIncomplete() {
		if (charParser == null) {
			return false;
		}

		return charParser.isEnableIncomplete();
	}

	public void setEnableIncomplete(boolean enableIncomplete) {
		if (charParser == null) {
			return;
		}

		charParser.setEnableIncomplete(enableIncomplete);
	}

	public boolean isSkipEmpty() {
		if (charParser == null) {
			return false;
		}

		return charParser.isSkipEmpty();
	}

	public void setSkipEmpty(boolean skipEmpty) {
		if (charParser == null) {
			return;
		}

		charParser.setSkipEmpty(skipEmpty);
	}

	public boolean isSkipLeadingBlanks() {
		if (charParser == null) {
			return false;
		}

		return charParser.isSkipLeadingBlanks();
	}

	public void setSkipLeadingBlanks(boolean skipLeadingBlanks) {
		if (charParser == null) {
			return;
		}

		charParser.setSkipLeadingBlanks(skipLeadingBlanks);
	}

	public boolean isSkipTrailingBlanks() {
		if (charParser == null) {
			return false;
		}

		return charParser.isSkipTrailingBlanks();
	}

	public void setSkipTrailingBlanks(boolean skipTrailingBlanks) {
		if (charParser == null) {
			return;
		}

		charParser.setSkipTrailingBlanks(skipTrailingBlanks);
	}
		
	public boolean byteMode() {
		return charParser == null;
	}
}
