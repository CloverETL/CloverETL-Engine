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
package org.jetel.util;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.security.GeneralSecurityException;

import org.apache.poi.POIXMLDocument;
import org.apache.poi.poifs.crypt.Decryptor;
import org.apache.poi.poifs.crypt.EncryptionInfo;
import org.apache.poi.poifs.filesystem.NPOIFSFileSystem;
import org.apache.poi.poifs.filesystem.OfficeXmlFileException;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.util.IOUtils;

/**
 * 
 * Utility class for determining the type of the supplied InputStream (XLS vs XLSX) and its encryption properties.
 * 
 * @author sgerguri (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Dec 6, 2011
 */
public class ExcelUtils {
	
	public enum ExcelType { XLS, XLSX, INVALID }
	
	public static ByteArrayInputStream getBufferedStream(InputStream is) throws IOException {
		ByteArrayInputStream bufferedStream = new ByteArrayInputStream(IOUtils.toByteArray(is));
		is.close();
		return bufferedStream;
	}
	
	public static ExcelType getStreamType(InputStream is) throws IOException {
		if (!(is.markSupported() || is instanceof PushbackInputStream)) {
			throw new IOException("Stream cannot be reset to initial position");
		}
		
		if (POIFSFileSystem.hasPOIFSHeader(is)) {
			return ExcelType.XLS;
		} else if (POIXMLDocument.hasOOXMLHeader(is)) {
			return ExcelType.XLSX;
		} else {
			return ExcelType.INVALID;
		}
	}
	
	public static InputStream getDecryptedXLSXStream(InputStream is, String password) throws IOException {
		try {
			NPOIFSFileSystem fs = new NPOIFSFileSystem(is);
			EncryptionInfo info = new EncryptionInfo(fs);
			Decryptor decryptor = Decryptor.getInstance(info);
			
			if (!decryptor.verifyPassword(password == null ? Decryptor.DEFAULT_PASSWORD : password)) {
				throw new IOException("Incorrect password");
			}
			
			return decryptor.getDataStream(fs);
		} catch (OfficeXmlFileException e) {
			// stream is not an OLE2 stream
			return null;
		} catch (FileNotFoundException e) {
			// stream does not contain encryption info part
			return null;		
		} catch (GeneralSecurityException e) {
			throw new IOException("Cannot process encrypted document");
		}
	}

}
