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
/**
 * 
 */
package org.jetel.util.exec;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.formatter.DataFormatter;
import org.jetel.exception.JetelException;
import org.jetel.graph.InputPort;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;

/**
 * Reads data from input port and sends them to specified output output stream,
 * which is supposed to be connected to process' input.
 * 
 * @see org.jetel.util.exec.ProcBox
 * @see org.jetel.util.exec.DataProducer
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 * @since 10/11/06 
 */
public class PortDataProducer implements DataProducer {
	private InputPort port;
	private DataRecord record;
	private DataFormatter formatter;
	private DataRecordMetadata metadata;

	static Log logger = LogFactory.getLog(PortDataProducer.class);

	/**
	 * Creates an instance for specified input port. Output data (ie process' input) is formatted according
	 * to metadata associated with the port.
	 * @param port Port supplying input data.
	 */
	public PortDataProducer(InputPort port) {
		this(port, port.getMetadata());
	}
	
	/**
	 * 
	 * @param port Port supplying input data.
	 * @param metadata Metadata describing output data (ie process' input).
	 */
	public PortDataProducer(InputPort port, DataRecordMetadata metadata) {
		this.port = port;
		this.metadata = metadata;
		formatter = new DataFormatter();
	}
	
	/**
	 * @see org.jetel.util.exec.DataProducer
	 */
	@Override
	public void setOutput(OutputStream stream) {
        formatter.init(metadata);
        try {
			formatter.setDataTarget(Channels.newChannel(stream));
		} catch (IOException e) {
			throw new RuntimeException("Unable to close previous data target.", e);
		}
        record = DataRecordFactory.newRecord(metadata);
	}
	
	/**
	 * @see org.jetel.util.exec.DataProducer
	 */
	@Override
	public boolean produce() throws JetelException	
	{
		try {
			if (port.readRecord(record) == null) {
				return false;
			}
			formatter.write(record);
		} catch (Exception e) {
			throw new JetelException("Error while reading input record", e);			
		}
		SynchronizeUtils.cloverYield();
		return true;
	}

	/**
	 * @throws IOException 
	 * @see org.jetel.util.exec.DataProducer
	 */
	@Override
	public void close() throws IOException {	
		formatter.close();
	}

}
