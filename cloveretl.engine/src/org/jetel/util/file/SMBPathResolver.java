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
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import jcifs.smb.Handler;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileInputStream;
import jcifs.smb.SmbFileOutputStream;

import org.jetel.component.fileoperation.SMBOperationHandler;

/**
 * Adds read/write support for Windows shares (SMB/CIFS protocol, UNC paths).
 * Implemented using jCIFS, accepts URLs in this format:
 *   smb://[[[domain;]username[:password]@]server[:port]/[[share/[dir/]file]]][?[param=value[param2=value2[...]]]
 * 
 * @author tkramolis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 23.5.2013
 */
public class SMBPathResolver implements CustomPathResolver {

	/** The name of SMB protocol. */
	public static final String SMB_PROTOCOL = "smb";
	/** The URL prefix of SMB URLs. */
	public static final String SMB_PROTOCOL_URL_PREFIX = SMB_PROTOCOL + "://";

	public static boolean isSmbUrl(String url) {
		return url != null && url.startsWith(SMB_PROTOCOL_URL_PREFIX);
	}
	
	@Override
	public InputStream getInputStream(URL contextURL, String input) throws IOException {
		if (isSmbUrl(input)) {
			return new SmbFileInputStream(input);
		}
		return null;
	}

	@Override
	public OutputStream getOutputStream(URL contextURL, String output, boolean appendData, int compressLevel) throws IOException {
		if (isSmbUrl(output)) {
			return new SmbFileOutputStream(output, appendData);
		}
		return null;
	}

	@Override
	public URL getURL(URL contextURL, String fileURL) throws MalformedURLException {
		if (isSmbUrl(fileURL)) {
			return new URL(contextURL, fileURL, new Handler());
		}
		throw new MalformedURLException("Not a SMB URL: " + fileURL);
	}

	@Override
	public boolean handlesURL(URL contextURL, String fileURL) {
		return isSmbUrl(fileURL);
	}

	@Override
	public List<String> resolveWildcardURL(URL contextURL, String fileURL) throws IOException {
		if (isSmbUrl(fileURL)) {
			List<SmbFile> resolvedFiles = SMBOperationHandler.resolve(fileURL);
			List<String> result = new ArrayList<String>(resolvedFiles.size());
			for (SmbFile file: resolvedFiles) {
				result.add(file.getURL().toString());
			}
			return result;
		}
		throw new MalformedURLException("Cannot handle '" + fileURL + "', only smb:// URL supported");
	}

}
