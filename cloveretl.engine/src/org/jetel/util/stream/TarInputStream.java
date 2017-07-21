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
package org.jetel.util.stream;

import java.io.IOException;
import java.io.InputStream;

/**
 * This class is a replacement of {@link com.ice.tar.TarInputStream}
 * with CLO-671 fixed.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Apr 29, 2013
 */
public class TarInputStream extends com.ice.tar.TarInputStream {

	public TarInputStream(InputStream is) {
		super(is);
	}

	public TarInputStream(InputStream is, int blockSize) {
		super(is, blockSize);
	}

	public TarInputStream(InputStream is, int blockSize, int recordSize) {
		super(is, blockSize, recordSize);
	}

    /**
     * Reads the next byte of data from the input stream. The value byte is
     * returned as an <code>int</code> in the range <code>0</code> to
     * <code>255</code>. If no byte is available because the end of the stream
     * has been reached, the value <code>-1</code> is returned. This method
     * blocks until input data is available, the end of the stream is detected,
     * or an exception is thrown.
     *
     * @return     the next byte of data, or <code>-1</code> if the end of the
     *             stream is reached.
     * @exception  IOException  if an I/O error occurs.
     * @see <a href="https://bug.javlin.eu/browse/CLO-671">CLO-671</a>
     */
	@Override
	public int read() throws IOException {
		int num = this.read( this.oneBuf, 0, 1 );
		if ( num == -1 ) {
			return num;
		} else {
			// CLO-671: convert negative bytes to positive
			// https://issues.apache.org/jira/browse/COMPRESS-23
			return (int) this.oneBuf[0] & 0xFF; 
		}
	}

}
