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
import java.nio.file.DirectoryStream;

import org.jetel.util.file.ArchiveUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 7. 9. 2016
 */
public class GzipDirectoryStream extends ArchiveDirectoryStream<InputStream, Object> {
	
	private boolean firstEntry;
	private static final Object ENTRY = new Object();

	/**
	 * @param parent
	 * @param glob
	 */
	protected GzipDirectoryStream(DirectoryStream<Input> parent) {
		super(parent, null);
	}

	@Override
	protected Object getNextEntry() throws IOException {
		if (firstEntry) {
			firstEntry = false;
			return ENTRY;
		} else {
			return null;
		}
	}

	@Override
	protected InputStream newArchiveInputStream(InputStream is) throws IOException {
		firstEntry = true;
		return ArchiveUtils.getGzipInputStream(is);
	}

	@Override
	protected boolean isDirectory(Object entry) {
		return false;
	}

	@Override
	protected String getName(Object entry) {
		return "";
	}

	@Override
	protected String getEntryUrl(String innerUrl, String entryName) {
		return "gzip:(" + innerUrl + ")";
	}

	@Override
	protected Input getEntryInput(Object entry) {
		// minor optimization
		return new ArchiveInput(getEntryUrl(currentInput.getAbsolutePath(), getName(entry)), archiveInputStream) {

			@Override
			protected InputStream wrap(InputStream is) throws IOException {
				return is; // no wrapping is necessary
			}
			
		};
	}


}

