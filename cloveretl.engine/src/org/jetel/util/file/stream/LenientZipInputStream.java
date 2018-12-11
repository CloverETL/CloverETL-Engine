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

import java.io.BufferedInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * CLO-13237:
 * 
 * This filter input stream can skip the beginning of a ZIP file, until it finds the first "local file header" (see
 * below) or reaches the limit.
 * 
 * Used for self-extracting archives - skips the executable part of the file.
 * 
 * <pre>
Overall .ZIP file format:

      [local file header 1]
      [encryption header 1]
      [file data 1]
      [data descriptor 1]
      . 
      .
      .
      [local file header n]
      [encryption header n]
      [file data n]
      [data descriptor n]
      [archive decryption header] 
      [archive extra data record] 
      [central directory header 1]
      .
      .
      .
      [central directory header n]
      [zip64 end of central directory record]
      [zip64 end of central directory locator] 
      [end of central directory record]
      
Local file header:

      local file header signature     4 bytes  (0x04034b50)
      version needed to extract       2 bytes
      general purpose bit flag        2 bytes
      compression method              2 bytes
      last mod file time              2 bytes
      last mod file date              2 bytes
      crc-32                          4 bytes
      compressed size                 4 bytes
      uncompressed size               4 bytes
      file name length                2 bytes
      extra field length              2 bytes

      file name (variable size)
      extra field (variable size)

https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT
 * </pre>
 * 
 * @author Milan (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 6. 12. 2018
 */
public class LenientZipInputStream extends FilterInputStream {

	private static final int MB = 1024 * 1024;

	/**
	 * Default limit, determines the maximum number of bytes that will be read before returning EOF.
	 */
	public static final int DEFAULT_LIMIT = 5 * MB;

	/**
	 * local file header signature
	 */
	private static final byte[] ZIP_LOCAL = { 0x50, 0x4b, 0x03, 0x04 };

	/** index for checking local file header signature */
	private int checkIndex;
	/** index for returning the buffered local file header after a successful check */
	private int outputIndex;

	/**
	 * The stream will return EOF after reaching the limit to prevent long reading of big files that are not ZIP
	 * archives.
	 * 
	 * Negative value means unlimited.
	 */
	private final int limit;

	private int counter = 0;

	/**
	 * Sets the limit for skipping to 5 MB.
	 * 
	 * The executable part is usually small, because it adds overhead to the archive,
	 * but it could be an installer with some images or license files.
	 * 
	 * @param is
	 */
	public LenientZipInputStream(InputStream is) {
		this(is, DEFAULT_LIMIT);
	}

	/**
	 * 
	 * @param is
	 * @param limit
	 *            negative value means unlimited
	 */
	public LenientZipInputStream(InputStream is, int limit) {
		super((is instanceof BufferedInputStream) ? is : new BufferedInputStream(is));
		this.limit = limit;
	}

	protected int limitReached() throws IOException {
		return -1;
		// throw new IOException("No ZIP entry found in the first " + limit + " bytes");
	}

	private int readByte() throws IOException {
		if ((limit >= 0) && (counter++ > limit)) {
			return limitReached();
		}

		return super.read(); // this is slow, parent InputStream must be buffered
	}

	@Override
	public int read() throws IOException {
		while (checkIndex < ZIP_LOCAL.length) { // loop until successful check or limit reached
			int c = readByte();
			if (c < 0) {
				return c; // EOF
			}
			if (c == ZIP_LOCAL[checkIndex]) { // matching byte
				checkIndex++;
			} else { // non-matching byte found
				checkIndex = 0; // start checking from the beginning again
			}
		}

		// local file header signature successfully checked

		if (outputIndex < ZIP_LOCAL.length) {
			return ZIP_LOCAL[outputIndex++];
		} else {
			return super.read();
		}
	}

	@Override
	public int read(byte[] buffer, int offset, int maxLength) throws IOException {
		if (outputIndex == ZIP_LOCAL.length) { // header already returned
			return super.read(buffer, offset, maxLength);
		}

		// header not completely returned
		int n = 0; // number of bytes read
		int maxBytes = Math.min(maxLength, ZIP_LOCAL.length);
		while (n < maxBytes) {
			int c = read();
			if (c < 0) {
				return (n > 0) ? n : -1; // EOF
			}
			buffer[offset + n++] = (byte) c;
		}
		return n;
	}

	public int getLimit() {
		return limit;
	}

}