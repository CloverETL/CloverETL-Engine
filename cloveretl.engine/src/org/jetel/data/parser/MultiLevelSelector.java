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

import java.nio.BufferUnderflowException;
import java.nio.CharBuffer;
import java.util.Properties;

import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;


/**
 * 
 * An interface to a "logic" implementation that is able to determine a record metadata in a flat file
 * by looking ahead in corresponding CharBuffer
 * 
 * @since Feb 2009
 * @author Pavel Najvar
 * @see UniversalMultiLevelParser
 */
public interface MultiLevelSelector {

	/**
	 * Initializes this type selector.
	 * @param metadata Specifies various data record types. The selector is supposed to operate on
	 * these types. Type selection (method choose()) returns index into this array.
	 * @param properties User-specified properties.
	 * @throws ComponentNotReadyException
	 */
	void init(DataRecordMetadata[] metadata, Properties properties) throws ComponentNotReadyException;

	/**
	 * A method that peeks info CharBuffer and reads characters until it can either determine metadata of the record
	 * which it reads, and thus will set nextRecordMetadataIndex to metadata pool specified in init() method, or runs out of data
	 * throwing the BufferUnderflowException. In such case the parent is forced to try to load more data and re-choose().
	 * 
	 * In order to avoid too many buffer underruns, the implementation is encouraged to at least guess the number of characters
	 * it needs to determine the type ({@link MultiLevelSelector#lookAheadCharacters()}.
	 * 
	 * choose() operation is called again by the parent with new data everytime a buffer underflow occurs. This selector can maintain its
	 * state between calls to choose(). This state must be discared each time the {@link reset()} method is called.
	 * 
	 * see {@link nextRecordMetadataIndex}
	 * 
	 * @param nextData
	 * @param decoder
	 * @return
	 * @throws JetelException
	 */
	void choose(CharBuffer data) throws BufferUnderflowException;

	/**
	 * Resets the internal state of the selector, if any 
	 * This method is called each time a new choice needs to be made
	 * 
	 * Usually this sets internal "nextRecordMetadataIndex" to -1, which means no choice
	 * otherwise the parent parser will be confused
	 */
	void reset();
	
	/**
	 * Returns the number of characters needed to decide (next) record type
	 * Usually it can be any fixed number of character, but dynamic lookahead size, depending on previous
	 * record type, is supported and encouraged whenever possible.
	 * 
	 * The number of characters is only informative and should the choose() method lack enough characters it can
	 * throw a {@link BufferUnderflowException} so that parent will try to load more data and re-choose
	 * 
	 * @return
	 */
	int lookAheadCharacters();
	
	/**
	 * Each call to choose() can instrument the parent to skip certain number of characters
	 * before attempting to parse a record according to metadata returned in choose() method
	 * @return
	 */
	int nextRecordOffset();
	
	/**
	 * returns type (metadata) of next found record (in choose())
	 * @return
	 */
	int nextRecordMetadataIndex(); 

	/**
	 * This method instruments the selector to find the offset
	 * of next record which is possibly parseable
	 * 
	 * It works similarily as choose(CharBuffer) but instead of setting metadata
	 * it simply set the position inside "data" of next possible record
	 * 
	 * Returns true if it was able to recover, exception if it needs more data or false
	 * if for any reason the recovery is impossible
	 * 
	 * @return
	 */
	void recoverToNextRecord(CharBuffer data) throws BufferUnderflowException, BadDataFormatException;
	
}
