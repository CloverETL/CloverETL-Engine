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
package org.jetel.util.string;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

/**
 * Character reader which allows reading content line by line into
 * specified buffer (any class implementing Appendable) - eg. StringBuffer/Builder, etc.<br/>
 * Capable of correctly identifying line boundaries with CR/LF/CR+LF delimiters.
 * 
 * Extends BufferedReader, thus no extra/additional buffering necessary
 * 
 * @author dpavlis, Javlin a.s. (info@cloveretl.com)
 *
 */
public class LineReader extends BufferedReader {
	private static final int CR='\n';
	private static final int LF='\r';
	
	private boolean signalCR = false;
	
	public LineReader(Reader reader){
		super(reader);
	}
	
	public LineReader(Reader reader, int bufSize){
		super(reader,bufSize);
	}
	

	/**
	 * Reads line of text terminated by LF, CR or CR+LF, appends line content
	 * to the buffer.
	 * 
	 * @param buffer
	 * @return length of the line just read or -1 if EOF
	 * @throws IOException
	 */
	public int readLine(Appendable buffer) throws IOException {
		int character;
		int length = 0;
		

		loop: 
		while ((character = read()) != -1) {
			switch (character) {
			case LF:
				if (signalCR){ //was CR and now is LF, ignore LF
					signalCR=false;
					break;
				}
				break loop;
			case CR:
				signalCR = true;
				break loop;
			default:
				buffer.append((char)character);
				length++;
				break;
			}
		}
		return character == -1 ? -1 : length;
	}
	
}
