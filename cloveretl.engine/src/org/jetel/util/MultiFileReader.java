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
package org.jetel.util;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.parser.Parser;
import org.jetel.exception.JetelException;

public class MultiFileReader {
	
    static Log logger = LogFactory.getLog(MultiFileReader.class);

	private Parser parser;
	private Iterator<String> filenameItor;
	private int fileSkip;
	private int maxRecCnt;
	private int fileMaxRecCnt;
	private int counter;
	private int fileCounter;
	private String filename;

	public MultiFileReader(Parser parser, String fileSpec, int skip, int fileSkip,
			int maxRecCnt, int fileMaxRecCnt) {
		this.parser = parser;
		WcardPattern pat = new WcardPattern();
		pat.addPattern(fileSpec, Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
		this.filenameItor = pat.filenames().iterator();
		this.fileSkip = fileSkip; 
		this.maxRecCnt = maxRecCnt;
		this.fileMaxRecCnt = fileMaxRecCnt;
		this.counter = 0;
		this.fileCounter = 0;
		filename = null;
		do {
			nextSource();
			try {
				counter += parser.skip(skip - counter);
			} catch (JetelException e) {
				logger.error("An error occured while skipping records in file " + filename +
						", the file will be ignored", e);
			}
		} while (counter < skip);
	}
	
	private boolean nextSource() {
		fileCounter = 0;
		ReadableByteChannel stream = null; 
		while (filenameItor.hasNext()) {
			filename = filenameItor.next();
			logger.debug("Opening input file " + filename);
			try {
				stream = FileUtils.getReadableChannel(filename);
				logger.debug("Reading input file " + filename);
				parser.setDataSource(stream);
				counter += parser.skip(fileSkip);
				return true;
			} catch (IOException e) {
				logger.error("Skipping unreadable file " + filename, e);
				continue;
			} catch (JetelException e) {
				logger.error("An error occured while skipping records in file " + filename +
						", the file will be ignored", e);
				continue;
			}
		}
		return false;
	}

	public DataRecord getNext(DataRecord record) throws JetelException {
		if (maxRecCnt > 0 && counter == maxRecCnt) {
			return null;
		}
		if (fileMaxRecCnt > 0 && fileCounter == fileMaxRecCnt) {
			if (!nextSource()) {
				return null;
			}
		}
		DataRecord rec;
		while (true) {
			try {
				rec = parser.getNext(record);
			} catch(JetelException e) {
            	counter++;
				// TODO handle bdfe locally(?)
            	throw e;
			}
			if (rec != null || !nextSource()) {
				break;
			}
		}
		if (rec != null) {
			counter++;
		}
		return rec;
	}		

	public void close() {
		parser.close();
	}
}
