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
package org.jetel.ctl;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.jetel.util.file.FileUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 29. 10. 2013
 */
public class SourceStringReader extends StringReader implements SourceCodeProvider {
	
	private final String source;

	/**
	 * @param s
	 */
	public SourceStringReader(String source) {
		super(source);
		this.source = source;
	}
	
	@Override
	public String getSourceCode() {
		return source;
	}
	
	/**
	 * Reads the <code>input</code> and returns its value as a String. 
	 * 
	 * @param input
	 * @return
	 * @throws IOException
	 */
	private static String read(Reader input) throws IOException {
		StringBuilder output = new StringBuilder();
		char[] buf = new char[8192];
		int length;
		while ((length = input.read(buf)) >= 0) {
			output.append(buf, 0, length);
		}
		return output.toString();
	}
	
	/**
	 * Constructs a new {@link SourceStringReader} by copying the supplied {@link Reader}.
	 * 
	 * @param input
	 * @return
	 * @throws IOException
	 */
	public static SourceStringReader fromReader(Reader input) throws IOException {
		try {
			return new SourceStringReader(read(input));
		} finally {
			FileUtils.close(input);
		}
	}
	
}
