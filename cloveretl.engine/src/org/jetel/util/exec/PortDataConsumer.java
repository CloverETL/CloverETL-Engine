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
package org.jetel.util.exec;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.parser.Parser;
import org.jetel.data.parser.TextParserFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.OutputPort;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;

/**
 * Reads data from input stream, which is supposed to be connected to process' output,
 * and sends them to specified output port.
 * 
 * @see org.jetel.util.exec.ProcBox
 * @see org.jetel.util.exec.DataConsumer
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 * @since 10/11/06 
 */
public class PortDataConsumer implements DataConsumer {
	private OutputPort port;
	private Parser parser;
	private DataRecord record;
	private DataRecordMetadata metadata;
	
	static Log logger = LogFactory.getLog(PortDataConsumer.class);

	/**
	 * Creates an instance for specified output port. Input data (ie process' output) is supposed to conform to the
	 * metadata associated with the port.
	 * @param port Output port receiving consumed data.
	 */
	public PortDataConsumer(OutputPort port) {
		this.port = port;
		this.metadata = port.getMetadata();
		createParser();
	}

	/**
	 * Ctor.
	 * @param port Output port receiving consumed data.
	 * @param metadata Metadata describing input data (ie process' output) format. 
	 */
	public PortDataConsumer(OutputPort port, DataRecordMetadata metadata) {
		this.port = port;
		this.metadata = metadata;
		createParser();
	}

	/**
	 * Ctor. Supposed to be used when output port and metadata describing input format doesn't provide
	 * enough info for creating parser by the instance itself.
	 * @param port Output port receiving consumed metadata.
	 * @param metadata Metadata describing input data (ie process' output) format.
	 * @param parser Parser to be used for parsing input data. It is opened and closed internally. 
	 */
	public PortDataConsumer(OutputPort port, DataRecordMetadata metadata, Parser parser) {
		this.port = port;
		this.metadata = metadata;
		this.parser = parser;
	}

	private void createParser() {
		parser = TextParserFactory.getParser(metadata);
	}

	/**
	 * @see org.jetel.util.exec.DataConsumer
	 */
	@Override
	public void setInput(InputStream stream) {		
		try {
			parser.init();
            parser.setDataSource(stream);
		} catch (ComponentNotReadyException e) {
			// this is not expected to happen 
			throw new RuntimeException("Unable to open parser", e);
		} catch (IOException e) {
			throw new RuntimeException("Unable to close previous data source.", e);
		}
		record = DataRecordFactory.newRecord(metadata);
		record.init();
	}

	/**
	 * @see org.jetel.util.exec.DataConsumer
	 */
	@Override
	public boolean consume() throws JetelException {
		if (parser.getNext(record) == null) {
			return false;
		}
		try {
			port.writeRecord(record);
		} catch (Exception e) {
			throw new JetelException("Error while writing output record", e);			
		}
		SynchronizeUtils.cloverYield();
		return true;
	}

	/**
	 * @throws IOException 
	 * @see org.jetel.util.exec.DataConsumer
	 */
	@Override
	public void close() throws IOException {
		parser.close();
	}

}
