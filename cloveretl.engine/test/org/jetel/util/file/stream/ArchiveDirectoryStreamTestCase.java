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
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.util.Arrays;

import org.jetel.test.CloverTestCase;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 29. 9. 2016
 */
public abstract class ArchiveDirectoryStreamTestCase extends CloverTestCase {

	protected abstract byte[] prepareData() throws IOException;
	
	public void testClose() throws IOException {
		URL contextUrl = null;
		byte[] data = prepareData();
		
		final MockInputStream mockInputStream = new MockInputStream(data);
		
		DirectoryStream<Input> parent = new CollectionDirectoryStream(contextUrl, Arrays.asList("something")) {

			@Override
			protected Input createInput(URL contextUrl, String url) {
				return new URLInput(contextUrl, url) {

					@Override
					public InputStream getInputStream() throws IOException {
						return mockInputStream;
					}
					
				};
			}
			
		};
		AbstractDirectoryStream<?> stream = openDirectoryStream(parent, "*");
		stream.next(); // opens the underlying InputStream
		stream.close(); // should close the underlying InputStream
		assertTrue(mockInputStream.isClosed());
	}
	
	protected abstract AbstractDirectoryStream<?> openDirectoryStream(DirectoryStream<Input> parent, String mask) throws IOException;

}
