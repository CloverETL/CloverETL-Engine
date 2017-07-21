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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.parser.Parser.DataSourceType;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.jetel.util.InputPortReadableChannelTest.InputPortMock;
import org.jetel.util.exec.PlatformUtils;
import org.jetel.util.file.FileUtils;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 25 Jan 2012
 */
public class ReadableChannelIteratorTest extends CloverTestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
	}
	
	public void testFileSourcePreferred() throws JetelException, ComponentNotReadyException, MalformedURLException {
		ReadableChannelIterator sourceIterator = new ReadableChannelIterator(null, FileUtils.getFileURL("."), "neco/neco.txt");
		sourceIterator.setPreferredDataSourceType(DataSourceType.FILE);
		sourceIterator.init();
		assertTrue(sourceIterator.next() instanceof File);
	}

	public void testNextChannel() throws JetelException, ComponentNotReadyException, MalformedURLException {
		ReadableChannelIterator sourceIterator = new ReadableChannelIterator(null, FileUtils.getFileURL("."), "neco/neco.txt");
		sourceIterator.setPreferredDataSourceType(DataSourceType.FILE);
		sourceIterator.init();
		assertNull(sourceIterator.nextChannel());
	}
	
	public void testUriPortReading() throws Exception {
		testPortReading(DataSourceType.URI,
				new String[] {
					"hdfs://example.com/neco/neco.txt",
					"/neco/neco.txt", // works only on Unix-like OS thanks to new File("/neco/neco.txt").getCanonicalFile() which would yield something like C:/neco/neco.txt on Win)
					"file:/neco/neco.txt",
					"file:///neco/neco.txt",
					"file:/C:/neco/neco.txt",
					"file:///C:/neco/neco.txt"
				},
				new URI[] {
					new URI("hdfs://example.com/neco/neco.txt"),
					PlatformUtils.isWindowsPlatform() ?
							getCanonicalFileURI("file:/", "/neco/neco.txt") // would be file:/D:/neco/neco.txt in my case
							: new URI("file:///neco/neco.txt"),
					PlatformUtils.isWindowsPlatform() ?
							getCanonicalFileURI("file:/", "/neco/neco.txt") :
							new URI("file:/neco/neco.txt"),
					PlatformUtils.isWindowsPlatform() ?
							getCanonicalFileURI("file:///", "/neco/neco.txt") :
							new URI("file:///neco/neco.txt"),
					new URI("file:/C:/neco/neco.txt"),
					new URI("file:///C:/neco/neco.txt")
				} );
		
		if (PlatformUtils.isWindowsPlatform()) {
			testPortReading(DataSourceType.URI,
					new String[] {
						"C:/neco/neco.txt", // paths staring with a drive letter work only on Windows thanks to File.listRoots() in the ReadableChannelIterator
						"c:\\neco\\neco.txt",
					},
					new URI[] {
						new URI("file:/C:/neco/neco.txt"),
						new URI("file:/C:/neco/neco.txt")  // c: becomes uppercase thanks to File(path).getCanonicalFile()
					} );
		}
		
		testPortReading(DataSourceType.URI, null,
				new String[] { "/neco.txt" },
				new URI[] {
					PlatformUtils.isWindowsPlatform() ? 
						getCanonicalFileURI("file:/", "/neco.txt") :
						new URI("file:/neco.txt")
				}
		);
		
		if (PlatformUtils.isWindowsPlatform()) {
			testPortReading(DataSourceType.URI, null,
					new String[] { "c:\\neco.txt" },
					new URI[] { new URI("file:/C:/neco.txt") }
			);
		}
	}

	private URI getCanonicalFileURI(String prefix, String path) throws URISyntaxException, IOException {
		return new URI(prefix + new File(path).getCanonicalPath().replace('\\', '/'));
	}
	
	public void testUriPortReadingRelativePath() throws Exception {
		testPortReading(DataSourceType.URI, new URL("file:/tmp"),
				new String[] { "neco/neco.txt" },
				new URI[] { new URI("file:/tmp/neco/neco.txt") }
		);
		
		if (PlatformUtils.isWindowsPlatform()) {
			testPortReading(DataSourceType.URI, new URL("file:/tmp"),
					new String[] { "c:\\neco.txt" },
					new URI[] { new URI("file:/C:/neco.txt") }
			);
		}
	}

	public void testFilePortReading() throws Exception {
		testPortReading(DataSourceType.FILE,
				new String[] {
					"file:///neco/neco.txt",
					"file:///D:/neco/neco.txt"
				},
				new File[] {
					new File("/neco/neco.txt").getCanonicalFile(),  // getCanonicalFile() makes this OS independent
					PlatformUtils.isWindowsPlatform() ? new File("D:/neco/neco.txt") : new File("/D:/neco/neco.txt")
				});
	}
	
	private static void testPortReading(DataSourceType preferredDataSourceType, String[] fileURLs, Object[] expectedDataSources) throws Exception {
		testPortReading(preferredDataSourceType, FileUtils.getFileURL("."), fileURLs, expectedDataSources);
	}
	
	private static void testPortReading(DataSourceType preferredDataSourceType, URL contextURL, String[] fileURLs, Object[] expectedDataSources) throws Exception {
		DataRecordMetadata metadata = new DataRecordMetadata("TestMetadata");
		metadata.addField(new DataFieldMetadata("field", null));
		
		DataRecord record = DataRecordFactory.newRecord(metadata);
		record.init();
		
		InputPortMock inputPort = new InputPortMock("portReadingield", fileURLs);
		
		ReadableChannelIterator sourceIterator = new ReadableChannelIterator(inputPort, contextURL, "port:$0.portReadingield:source");
		sourceIterator.setPreferredDataSourceType(preferredDataSourceType);
		sourceIterator.init();
		for (int i = 0; i < fileURLs.length; i++) {
			Object actualDataSource = sourceIterator.next();
			assertEquals("Unexpected data source for file URL " + fileURLs[i], expectedDataSources[i], actualDataSource);
		}
		assertNull(sourceIterator.next());
	}

}
