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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Objects;

import org.jetel.component.fileoperation.CloverURI;
import org.jetel.component.fileoperation.URIUtils;
import org.jetel.data.parser.Parser.DataSourceType;
import org.jetel.util.file.FileUtils;

class URLInput extends AbstractInput {
	
	private final URL contextUrl;
	private final String file;

	/**
	 * @param file
	 */
	public URLInput(URL contextUrl, String file) {
		Objects.requireNonNull(file);
		this.contextUrl = contextUrl;
		this.file = file;
	}

	@Override
	public Object getPreferredInput(DataSourceType type) throws IOException {
		switch (type) {
		case STREAM:
			return FileUtils.getInputStream(contextUrl, file);
		case CHANNEL:
			return FileUtils.getReadableChannel(contextUrl, file);
		case URI:
			try {
				try {
					return CloverURI.createSingleURI(contextUrl != null ? contextUrl.toURI() : null, file).getAbsoluteURI().toURI();
				} catch (Exception e) {
					// ignore
				}
				URL url = FileUtils.getFileURL(contextUrl, file);
				return URIUtils.toURI(url);
			} catch (URISyntaxException | MalformedURLException ex) {
				throw new IOException("Invalid URL", ex);
			} catch (Exception e) {
				// DO NOTHING - just try to open a stream based on the currentFileName in the next step
			}
		case FILE:
			try {
				File f = FileUtils.getJavaFile(contextUrl, file);
				return f;
			} catch (Exception e) {
				//DO NOTHING - just try to prepare a data source in other way
			}
		}
		return null;
	}

	@Override
	public String getAbsolutePath() {
		if (FileUtils.STD_CONSOLE.equals(file)) {
			return file;
		}
		try {
			return FileUtils.getFileURL(contextUrl, file).toString();
		} catch (MalformedURLException e) {
			return file;
		}
	}
	
}