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
package org.jetel.util.file;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.compressors.z.ZCompressorInputStream;
import org.jetel.data.Defaults;
import org.jetel.enums.ArchiveType;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 14.11.2013
 */
public class ArchiveUtils {

	public static final byte ZIP_HEADER[] = new byte[] {0x50, 0x4b};
	public static final byte GZIP_HEADER[] = new byte[] {0x1f, (byte)0x8b};
	
	/**
	 * A file produced by the {@code compress} Linux utility.
	 */
	public static final byte[] Z_COMPRESSOR_HEADER = new byte[] {0x1F, (byte) 0x9D};
	public static final byte TAR_HEADER[] = new byte[] {0x75, 0x73, 0x74, 0x61, 0x72};
	
	private static final int TAR_HEADER_OFFSET = 257;
	
	public static final int PUSH_BACK_SIZE = TAR_HEADER_OFFSET + TAR_HEADER.length;
	
	private static final int Z_COMPRESSOR_HEADER_LENGTH = Z_COMPRESSOR_HEADER.length;
	
	public static ArchiveType getArchiveType(PushbackInputStream stream) throws IOException {
		
		byte buf[] = new byte[ZIP_HEADER.length];
		int len = stream.read(buf);
		if (len > 0) {
			stream.unread(buf, 0, len);
		}
		if (Arrays.equals(ZIP_HEADER, buf)) {
			return ArchiveType.ZIP;
		}
		if (Arrays.equals(GZIP_HEADER, buf) || Arrays.equals(Z_COMPRESSOR_HEADER, buf)) {
			return ArchiveType.GZIP;
		}
		buf = new byte[TAR_HEADER.length + TAR_HEADER_OFFSET];
		len = stream.read(buf);
		if (len > 0) {
			stream.unread(buf, 0, len);
		}
		if (Arrays.equals(TAR_HEADER, Arrays.copyOfRange(buf, TAR_HEADER_OFFSET, buf.length))) {
			return ArchiveType.TAR;
		}
		return null;
	}
	
	/**
	 * CLO-5810:
	 * 
	 * Returns a {@link GZIPInputStream} or a {@link ZCompressorInputStream}
	 * depending on the magic file header.
	 * 
	 * @param innerStream
	 * @return new archive {@link InputStream}
	 * @throws IOException if reading the file header fails
	 */
	public static InputStream getGzipInputStream(InputStream innerStream) throws IOException {
		return getGzipInputStream(innerStream, Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
	}
	
	public static InputStream getGzipInputStream(InputStream innerStream, int bufferSize) throws IOException {
    	byte[] magic = new byte[Z_COMPRESSOR_HEADER_LENGTH];
    	int len = 0;
		if (innerStream.markSupported()) {
			innerStream.mark(Z_COMPRESSOR_HEADER_LENGTH);
	    	len = innerStream.read(magic);
	    	innerStream.reset();
		} else {
			PushbackInputStream pushBackStream = null;
			try {
				// two bytes pushback buffer for magic header
		    	pushBackStream = new PushbackInputStream(innerStream, Z_COMPRESSOR_HEADER_LENGTH);
		    	len = pushBackStream.read(magic);
		    	if (len > 0) {
		    		pushBackStream.unread(magic, 0, len);
		    	}
		    	innerStream = pushBackStream;
			} catch (IOException ioe) {
				FileUtils.closeQuietly(pushBackStream);
				throw ioe;
			}
		}
    	if ((len == Z_COMPRESSOR_HEADER_LENGTH) && Arrays.equals(magic, Z_COMPRESSOR_HEADER)) {
    		return new ZCompressorInputStream(innerStream);
    	}
        return new GZIPInputStream(innerStream, bufferSize);
	}
}
