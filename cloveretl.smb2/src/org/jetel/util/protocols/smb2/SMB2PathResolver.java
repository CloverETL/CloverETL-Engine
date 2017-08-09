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
package org.jetel.util.protocols.smb2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.jetel.component.fileoperation.CloverURI;
import org.jetel.component.fileoperation.FileManager;
import org.jetel.component.fileoperation.SingleCloverURI;
import org.jetel.util.file.CustomPathResolver;
import org.jetel.util.file.FileMessages;

/**
 * Adds read/write support for Windows shares (SMB/CIFS protocol, UNC paths).
 * Implemented using smbj, accepts URLs in this format:
 *   smb2://[[[domain;]username[:password]@]server[:port]/[[share/[dir/]file]]][?[param=value[param2=value2[...]]]
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 7.8.2017
 */
public class SMB2PathResolver implements CustomPathResolver {

	/** The name of SMB protocol. */
	public static final String SMB2_PROTOCOL = "smb2"; //$NON-NLS-1$
	/** The URL prefix of SMB URLs. */
	public static final String SMB_PROTOCOL_URL_PREFIX = SMB2_PROTOCOL + "://"; //$NON-NLS-1$
	
	public static boolean isSmbUrl(String url) {
		return (url != null) && url.startsWith(SMB_PROTOCOL_URL_PREFIX);
	}
	
	@Override
	public InputStream getInputStream(URL contextURL, String input) throws IOException {
		if (isSmbUrl(input)) {
			return getURL(contextURL, input).openConnection().getInputStream();
		}
		return null;
	}

	@Override
	public OutputStream getOutputStream(URL contextURL, String output, boolean appendData, int compressLevel) throws IOException {
		if (isSmbUrl(output)) {
			URL url = getURL(contextURL, output);
			SMB2URLConnection connection = (SMB2URLConnection) url.openConnection();
			connection.getOutputStream(appendData);
		}
		return null;
	}

	@Override
	public URL getURL(URL contextURL, String fileURL) throws MalformedURLException {
		if (isSmbUrl(fileURL)) {
			return new URL(contextURL, fileURL, SMB2StreamHandler.INSTANCE);
		}
		throw new MalformedURLException(MessageFormat.format(FileMessages.getString("SMBPathResolver_not_a_smb_url"), fileURL)); //$NON-NLS-1$
	}

	@Override
	public boolean handlesURL(URL contextURL, String fileURL) {
		return isSmbUrl(fileURL);
	}

	@Override
	public List<String> resolveWildcardURL(URL contextURL, String fileURL) throws IOException {
		if (isSmbUrl(fileURL)) {
			URL url = getURL(contextURL, fileURL);
			try {
				List<SingleCloverURI> resolved = FileManager.getInstance().defaultResolve(CloverURI.createSingleURI(url.toURI()));
				List<String> result = new ArrayList<>(resolved.size());
				for (SingleCloverURI uri: resolved) {
					result.add(uri.toURI().toString());
				}
				return result;
			} catch (URISyntaxException e) {
				throw new IOException(e);
			}
		}
		throw new MalformedURLException(MessageFormat.format(FileMessages.getString("SMBPathResolver_cannot_handle_url"), fileURL)); //$NON-NLS-1$
	}
	
}
