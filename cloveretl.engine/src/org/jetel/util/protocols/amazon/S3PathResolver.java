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
package org.jetel.util.protocols.amazon;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.jetel.component.fileoperation.CloverURI;
import org.jetel.component.fileoperation.FileManager;
import org.jetel.component.fileoperation.Info;
import org.jetel.component.fileoperation.S3OperationHandler;
import org.jetel.component.fileoperation.SingleCloverURI;
import org.jetel.component.fileoperation.result.ResolveResult;
import org.jetel.util.file.CustomPathResolver;

/**
 * URL resolver for S3 protocol.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 19. 3. 2015
 */
public class S3PathResolver implements CustomPathResolver {
	
	private final FileManager manager = FileManager.getInstance();
	
	public static final String S3_PROTOCOL_URL_PREFIX = S3OperationHandler.S3_SCHEME + "://"; //$NON-NLS-1$

	/**
	 * Returns {@code true} if the input string is not null
	 * and starts with "s3://".
	 * 
	 * @param url
	 * @return {@code true} if {@code url} starts with "s3://"
	 */
	private static boolean isS3(String url) {
		return (url != null) && url.startsWith(S3_PROTOCOL_URL_PREFIX);
	}

	@Override
	public InputStream getInputStream(URL contextURL, String input) throws IOException {
		if (handlesURL(contextURL, input)) {
			return getURL(contextURL, input).openConnection().getInputStream();
		}
		return null;
	}

	@Override
	public OutputStream getOutputStream(URL contextURL, String output, boolean appendData, int compressLevel)
			throws IOException {
		if (handlesURL(contextURL, output)) {
			if (appendData) {
				throw new IOException("Appending is not supported by S3");
			}
			return getURL(contextURL, output).openConnection().getOutputStream();
		}
		return null;
	}

	@Override
	public URL getURL(URL contextURL, String fileURL) throws MalformedURLException {
		return new URL(contextURL, fileURL, S3StreamHandler.INSTANCE);
	}

	@Override
	public boolean handlesURL(URL contextURL, String fileURL) {
		return isS3(fileURL);
	}

	@Override
	public List<String> resolveWildcardURL(URL contextURL, String fileURL) throws IOException {
		if (handlesURL(contextURL, fileURL)) {
			URL url = getURL(contextURL, fileURL);
			CloverURI cloverUri = CloverURI.createURI(url.toString());
			ResolveResult resolveResult = manager.resolve(cloverUri);
			if (!resolveResult.success()) {
				throw new IOException(resolveResult.getFirstError());
			}
			List<SingleCloverURI> resolved = resolveResult.getResult();
			List<String> result = new ArrayList<>(resolved.size());
			for (SingleCloverURI resultUri: resolved) {
				if ((resultUri instanceof Info) && ((Info) resultUri).isDirectory()) {
					continue; // CLO-9619
				}
				result.add(resultUri.toURI().toString());
			}
			return result;
		}
		
		return null;
	}

}
