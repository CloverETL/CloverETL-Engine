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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.jetel.exception.JetelException;

/**
 * Reads data from input stream, which is supposed to be connected to process' output or more usually to error output,
 * and stores the data so that they may be retreived as a string.
 * 
 * @see org.jetel.util.exec.ProcBox
 * @see org.jetel.util.exec.DataConsumer
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 * @since 10/11/06 
 */
public class StringDataConsumer implements DataConsumer {
	private int maxLines;
	private int linesRead;
	private StringBuilder msg = new StringBuilder();
	private BufferedReader reader;

	/**
	 * Sole ctor.
	 * 
	 * @param maxLines Max count of stored lines. 0 for all. Excessive lines will be discarded. 
	 */
	public StringDataConsumer(int maxLines) {
		this.maxLines = maxLines;
		linesRead = 0;
	}

	/**
	 * @see org.jetel.util.exec.DataConsumer
	 */
	@Override
	public void setInput(InputStream stream) {
		reader = new BufferedReader(new InputStreamReader(stream));
	}

	/**
	 * @see org.jetel.util.exec.DataConsumer
	 */
	@Override
	public boolean consume() throws JetelException {
		String line;
		try {
			line = reader.readLine();
		} catch (IOException e) {
			throw new JetelException("Error while reading input data", e);
		}
		if (line == null) {
			return false;
		}

		linesRead++;
		if (maxLines != 0 && linesRead > maxLines) {
			return true;
		}

		if (maxLines != 0 && linesRead == maxLines + 1) {
			msg.append(line).append("\n...\n");	// last line to remember
		} else {
			msg.append(line).append("\n");
		}
		return true;
	}

	/**
	 * @see org.jetel.util.exec.DataConsumer
	 */
	@Override
	public void close() {
	}

	/**
	 * Retrieve consumed data in the form of a string. 
	 * @return The string.
	 */
	public String getMsg() {
		return msg.toString();
	}

}
