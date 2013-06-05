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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import jcifs.smb.Handler;
import jcifs.smb.SmbException;
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
	public static final String SMB_PROTOCOL = "smb"; //$NON-NLS-1$
	/** The URL prefix of SMB URLs. */
	public static final String SMB_PROTOCOL_URL_PREFIX = SMB_PROTOCOL + "://"; //$NON-NLS-1$

	public static boolean isSmbUrl(String url) {
		return url != null && url.startsWith(SMB_PROTOCOL_URL_PREFIX);
	}
	
	@Override
	public InputStream getInputStream(URL contextURL, String input) throws IOException {
		if (isSmbUrl(input)) {
			try {
				return new SmbFileInputStream(SMBOperationHandler.decodeURI(new URI(input)));
			} catch (SmbException e) {
				throw new SMBException(MessageFormat.format(FileMessages.getString("SMBPathResolver_failed_to_open_input_stream"), input), e); //$NON-NLS-1$
			} catch (URISyntaxException e) {
				throw new SMBException(MessageFormat.format(FileMessages.getString("SMBPathResolver_failed_to_open_input_stream"), input), e); //$NON-NLS-1$
			}
		}
		return null;
	}

	@Override
	public OutputStream getOutputStream(URL contextURL, String output, boolean appendData, int compressLevel) throws IOException {
		if (isSmbUrl(output)) {
			try {
				return new SmbFileOutputStream(SMBOperationHandler.decodeURI(new URI(output)), appendData);
			} catch (SmbException e) {
				throw new SMBException(MessageFormat.format(FileMessages.getString("SMBPathResolver_failed_to_open_output_stream"), output), e); //$NON-NLS-1$
			} catch (URISyntaxException e) {
				throw new SMBException(MessageFormat.format(FileMessages.getString("SMBPathResolver_failed_to_open_output_stream"), output), e); //$NON-NLS-1$
			}
		}
		return null;
	}

	@Override
	public URL getURL(URL contextURL, String fileURL) throws MalformedURLException {
		if (isSmbUrl(fileURL)) {
			return new URL(contextURL, fileURL, new Handler());
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
			try {
				List<SmbFile> resolvedFiles = SMBOperationHandler.resolve(SMBOperationHandler.decodeURI(new URI(fileURL)));
				List<String> result = new ArrayList<String>(resolvedFiles.size());
				for (SmbFile file: resolvedFiles) {
					result.add(file.getURL().toString());
				}
				return result;
			} catch (SmbException e) {
				throw new SMBException(MessageFormat.format(FileMessages.getString("SMBPathResolver_filed_to_resolve_wildcards"), fileURL), e); //$NON-NLS-1$
			} catch (URISyntaxException e) {
				throw new SMBException(MessageFormat.format(FileMessages.getString("SMBPathResolver_filed_to_resolve_wildcards"), fileURL), e); //$NON-NLS-1$
			}
		}
		throw new MalformedURLException(MessageFormat.format(FileMessages.getString("SMBPathResolver_cannot_handle_url"), fileURL)); //$NON-NLS-1$
	}
	
	/** 
	 * Because jcifs.smb.SmbException never contains info about what URL was handled when error occurred,
	 * this class is used to wrap any jcifs.smb.SmbException thrown and contains culprit URL in its message.
	 */
	public static class SMBException extends IOException {
		
		private static final long serialVersionUID = 1L;
		
		public SMBException(String msg, Throwable cause) {
			super(msg, cause);
		}
	}
	
}
