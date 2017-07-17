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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.JetelException;
import org.jetel.exception.JetelRuntimeException;

/**
 * Reads data from input stream, which is supposed to be connected to process' output or more usually to error output,
 * and writes the data using logger.
 * 
 * Moreover, this DataConsumer allows to cache all logged data and return them, see {@link #getMsg()}.
 * This functionality is similar to {@link StringDataConsumer}.
 * 
 * @see org.jetel.util.exec.ProcBox
 * @see org.jetel.util.exec.DataConsumer
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 * @since 10/11/06 
 */
public class LoggerDataConsumer implements DataConsumer {
	/**
	 * Debug log level.
	 */
	public static final int LVL_DEBUG = 0;
	/**
	 * Warning log level.
	 */
	public static final int LVL_WARN = 1;
	/**
	 * Error log level.
	 */
	public static final int LVL_ERROR = 2;
	/**
	 * Info log level.
	 */
	public static final int LVL_INFO = 3;
	// TODO add more levels(?)
	
	private int level;
	private int maxLines;
	private int linesRead;
	private BufferedReader reader;

	/** Indicates, whether the logged data should be available using {@link #getMsg()} method. */
	private boolean cacheLoggedData = false;
	/** All incoming data are stored to this variable for furhter usage, see {@link #cacheLoggedData} */
	private StringBuilder msg = new StringBuilder();

	static Log logger = LogFactory.getLog(PortDataConsumer.class);

	/** Sole ctor.
	 * 
	 * @param level Log level.
	 * @param maxLines Max count of lines written to log. 0 for all. Excessive lines will be discarded. 
	 */
	public LoggerDataConsumer(int level, int maxLines) {
		switch (level) {
		case LVL_DEBUG:
		case LVL_WARN:
		case LVL_ERROR:
		case LVL_INFO:
			this.level = level;
			break;
		default:
			this.level = LVL_ERROR;
		}
		this.maxLines = maxLines;
		linesRead = 0;
	}

	public void setCacheLoggedData(boolean cacheLoggedData) {
		this.cacheLoggedData = cacheLoggedData;
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

		if (cacheLoggedData) {
			if (maxLines != 0 && linesRead == maxLines + 1) {
				msg.append(line).append("\n...\n");	// last line to remember
			} else {
				msg.append(line).append("\n");
			}
		}

		switch (level) {
		case LVL_DEBUG:
			logger.debug(line);
			break;
		case LVL_WARN:
			logger.warn(line);
			break;
		case LVL_ERROR:
			logger.error(line);
			break;
		case LVL_INFO:
			logger.info(line);
			break;
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
		if (cacheLoggedData) {
			return msg.toString();
		} else {
			throw new JetelRuntimeException("Message is not available.");
		}
	}

}
