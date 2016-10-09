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
package org.jetel.util.file.stream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ReadableByteChannel;

import org.jetel.data.parser.Parser.DataSourceType;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5. 9. 2016
 */
public interface Input {
	
	public Object getPreferredInput(DataSourceType type) throws IOException;
	
	public InputStream getInputStream() throws IOException;
	
	public ReadableByteChannel getByteChannel() throws IOException;
	
	/**
	 * Returns the absolute path to the file as a string.
	 * Used in autofilling or as an input to FileUtils.getInputStream().
	 * 
	 * For <b>local files</b>, returns the absolute path on a file system (not a <code>"file:"</code> URL):
	 * <p><code>
	 * C:\Users\John\data\file.txt<br>
	 * /home/john/data/file.txt
	 * </code></p>
	 * 
	 * For <b>sandbox and remote files</b>, returns the URL as a string:
	 * <p><code>
	 * sandbox://sandboxcode/path/to/file.txt<br>
	 * ftp://user:password@hostname/path/to/file.txt
	 * </code></p>
	 * 
	 * @return absolute path to a file (local path or URL)
	 */
	public String getAbsolutePath();
	
//	/**
//	 * Returns the last segment of the file path - the name of the file.
//	 * @return
//	 */
//	public String getName();
//	
//	public long getLastModified();
//	
//	public long getSize();
	
}
