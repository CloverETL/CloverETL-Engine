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
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;

import org.jetel.exception.JetelException;

/**
 * Reads data from input stream, which is supposed to be connected to process' output,
 * and writes them using an instance of class Writer. The writer may not be based on a file,
 * so the name of this class is kind of misnomer.
 * 
 * @see org.jetel.util.exec.ProcBox
 * @see org.jetel.util.exec.DataConsumer
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 * @since 10/11/06 
 */
public class FileDataConsumer implements DataConsumer {
	Writer writer;
	BufferedReader reader;

	/**
	 * Sole ctor.
	 * @param writer Writer to be used to write input data.
	 */
	public FileDataConsumer(Writer writer) {
		this.writer = new BufferedWriter(writer);
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
		try {
			synchronized (writer) {
				writer.write(line);
				writer.write("\n");
			}
		} catch (IOException e) {
			throw new JetelException("Error while writing data", e);				
		}
		return true;
	}

	/**
	 * @see org.jetel.util.exec.DataConsumer
	 */
	@Override
	public void close() {
	}

}
