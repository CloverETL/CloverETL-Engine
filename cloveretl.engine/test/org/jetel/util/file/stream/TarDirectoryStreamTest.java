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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 3. 10. 2016
 */
public class TarDirectoryStreamTest extends ArchiveDirectoryStreamTestCase {

	@Override
	protected AbstractDirectoryStream<?> openDirectoryStream(DirectoryStream<Input> parent, String mask) throws IOException {
		return new TarDirectoryStream(parent, mask);
	}

	@Override
	protected byte[] prepareData() throws IOException {
		try (
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			TarArchiveOutputStream tos = new TarArchiveOutputStream(os);
		) {
			byte[] bytes = "hello world".getBytes();
			TarArchiveEntry entry = new TarArchiveEntry("file.txt");
			entry.setSize(bytes.length);
			tos.putArchiveEntry(entry);
			tos.write(bytes);
			tos.closeArchiveEntry();
			tos.close();
			return os.toByteArray();
		}	
	}
}
