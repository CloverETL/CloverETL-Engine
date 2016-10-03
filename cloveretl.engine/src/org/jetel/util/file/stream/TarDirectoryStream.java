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

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5. 9. 2016
 */
public class TarDirectoryStream extends ArchiveDirectoryStream<ArchiveInputStream, ArchiveEntry> {

	/**
	 * @param parent
	 * @param glob
	 */
	public TarDirectoryStream(DirectoryStream<Input> parent, String glob) {
		super(parent, glob);
	}

	@Override
	protected ArchiveEntry getNextEntry() throws IOException {
		return archiveInputStream.getNextEntry();
	}

	@Override
	protected ArchiveInputStream newArchiveInputStream(InputStream is) throws IOException {
		return new TarArchiveInputStream(is);
	}

	@Override
	protected boolean isDirectory(ArchiveEntry entry) {
		return entry.isDirectory();
	}

	@Override
	protected String getName(ArchiveEntry entry) {
		return entry.getName();
	}

	@Override
	protected String getEntryUrl(String innerUrl, String entryName) {
		return "tar:(" + innerUrl + ")#" + entryName;
	}
	
}
