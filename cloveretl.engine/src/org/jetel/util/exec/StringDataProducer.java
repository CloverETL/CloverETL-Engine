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
import java.io.OutputStream;

import org.jetel.exception.JetelException;

/**
 * Data producerer supplying a string to output stream,
 * which is suposed to be connected to process' input.
 * 
 * @see org.jetel.util.exec.ProcBox
 * @see org.jetel.util.exec.DataProducer
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 * @since 10/11/06 
 */
public class StringDataProducer implements DataProducer {
	private String input;
	private OutputStream stream;

	/**
	 * Sole ctor.
	 * @param input String to be suplied to output (ie process' input).
	 */
	public StringDataProducer(String input) {
		this.input = input;
	}

	/**
	 * @see org.jetel.util.exec.DataProducer
	 */
	@Override
	public void setOutput(OutputStream stream) {
		this.stream = stream;
	}

	/**
	 * @see org.jetel.util.exec.DataProducer
	 */
	@Override
	public boolean produce() throws JetelException {
		// all data are suppplied by one call to this method
		if (input == null) {
			return false;
		}
		try {
			stream.write(input.getBytes());
		} catch (IOException e) {
			throw new JetelException("Error writing data to stream", e);			
		}
		input = null;
		return true;
	}

	/**
	 * @see org.jetel.util.exec.DataProducer
	 */
	@Override
	public void close() {
	}

}
