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
package org.jetel.util.string;

import java.io.IOException;
import java.io.Reader;

/**
 * This class implements Reader interface on top of CharSequence - i.e.
 * it enables reading chars from String,StringBuffer/Builder,CharBuffer
 *
 * @author david pavlis <david.pavlis@opensys.eu>
 * @since       Oct 24, 2007
 */

public class CharSequenceReader extends Reader {

	CharSequence input;
	int pos;
	final int length;
	
	public CharSequenceReader(CharSequence input){
		this.input=input;
		this.length=input.length();
		pos=0;
	}
	
	
	/* (non-Javadoc)
	 * @see java.io.Reader#close()
	 */
	@Override
	public void close() throws IOException {
		//no need to close anything

	}

	/* (non-Javadoc)
	 * @see java.io.Reader#read(char[], int, int)
	 */
	@Override
	public int read(char[] cbuf, int off, int len) throws IOException {
		if (pos<length){
			int i=0;
			while(i<len){
				cbuf[off++]=input.charAt(pos++);
				i++;
				if (pos>=this.length) break;
			}
			return i;
		}		
		return -1;
	}
	
	@Override
	public void reset() throws IOException{
		super.reset();
		pos=0;
	}

}
