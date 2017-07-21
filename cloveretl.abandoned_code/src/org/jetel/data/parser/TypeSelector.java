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
import java.nio.charset.CharsetDecoder;
import java.util.Properties;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Interface to be implemented by type selectors for multi-level parser.
 * It is used by the parser to recognize which type is expected.  
 * @since 15/12/2006
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 * @see MultiLevelParser
 */
public interface TypeSelector {

	/**
	 * Initializes type selector.
	 * @param metadata Specifies various data record types. The selector is supposed to operate on
	 * these types. Type selection (method choose()) returns index into this array.
	 * @param properties User-specified properties.
	 * @return Length of data required to select record-type.
	 * @throws ComponentNotReadyException
	 */
	int init(DataRecordMetadata[] metadata, Properties properties) throws ComponentNotReadyException;

	/**
	 * Selects data type.
	 * @param nextData Initial part of next record. When enough input data is available its size will
	 * be greater or equal to the value returned by the init() method.
	 * @param decoder Decoder to be used when selector needs to convert input data to string.
	 * @return
	 * @throws JetelException
	 */
	int choose(ByteBuffer nextData, CharsetDecoder decoder) throws JetelException;

	/**
	 * This method is called whenever new record is read.
	 * @param record The new record.
	 */
	void presentRecord(DataRecord record);
	
	/**
	 * Nomen omen. 
	 */
	void finish();
}
