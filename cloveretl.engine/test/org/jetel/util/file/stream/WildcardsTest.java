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
import java.io.InputStream;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.test.CloverTestCase;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26. 9. 2016
 */
public class WildcardsTest extends CloverTestCase {
	
	private static Log LOG = LogFactory.getLog(WildcardsTest.class);
	
	private URL contextUrl;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		this.contextUrl = null;
	}

	/**
	 * Test method for {@link org.jetel.util.file.stream.Wildcards#newDirectoryStream(java.net.URL, java.lang.String)}.
	 */
	public void testNewDirectoryStream() throws IOException {
		URL testScenarios = new File("../cloveretl.test.scenarios").toURI().toURL();

		try (DirectoryStream<Input> stream = Wildcards.newDirectoryStream(contextUrl, "")) {
			assertEquals(0, size(stream));
		}
		
		try (DirectoryStream<Input> stream = Wildcards.newDirectoryStream(contextUrl, "*")) {
			assertTrue(size(stream) > 0);
		}
		
		// local files
		try (DirectoryStream<Input> stream = Wildcards.newDirectoryStream(contextUrl, "test/*.fmt")) {
			assertTrue(size(stream) > 0);
		}
		
		// FTP
		try (DirectoryStream<Input> stream = Wildcards.newDirectoryStream(contextUrl, "ftp://test:test@koule/data-in/URL*.txt")) {
			assertEquals(4, size(stream));
		}
		
		// SFTP
		try (DirectoryStream<Input> stream = Wildcards.newDirectoryStream(contextUrl, "sftp://test:test@koule/home/test/data-in/URL*.txt")) {
			assertEquals(4, size(stream));
		}

		// multiple sources
		try (DirectoryStream<Input> stream = Wildcards.newDirectoryStream(contextUrl, "ftp://test:test@koule/data-in/URL*.txt;sftp://test:test@koule/home/test/data-in/URL*.txt")) {
			assertEquals(8, size(stream));
		}
		
		{
			Pattern pattern = Pattern.compile("#innerfolder\\d/URLIn\\d{2}\\.txt");
			
			// ZIP on FTP
			try (DirectoryStream<Input> stream = Wildcards.newDirectoryStream(contextUrl, "zip:(ftp://test:test@koule/data-in/file?.zip)#*.txt")) {
				int count = 0;
				for (Input input: stream) {
					count++;
					String path = input.getAbsolutePath();
					String fragment = path.substring(path.indexOf('#'));
					LOG.debug(fragment);
					assertTrue(pattern.matcher(fragment).matches());
				}
				assertEquals(4, count);
			}
	
			// TAR on SFTP
			try (DirectoryStream<Input> stream = Wildcards.newDirectoryStream(contextUrl, "tar:(sftp://test:test@koule/home/test/data-in/tarfile??.tar)#*.txt")) {
				int count = 0;
				for (Input input: stream) {
					count++;
					String path = input.getAbsolutePath();
					String fragment = path.substring(path.indexOf('#'));
					LOG.debug(fragment);
					assertTrue(pattern.matcher(fragment).matches());
				}
				assertEquals(4, count);
			}
		}
		

		// TAR.GZ on SMB
		try (DirectoryStream<Input> stream = Wildcards.newDirectoryStream(contextUrl, "tgz:(smb://SMBTest:p%40ss%7B%2F%7D@virt-orange/SMBTestPub/CLO-6584/*.tar.gz)#*.txt")) {
			assertEquals(2, size(stream));
		}

		// TAR on local FS
		try (DirectoryStream<Input> stream = Wildcards.newDirectoryStream(testScenarios, "tar:(./data-in/fileFourFiles*.tar)#*.txt")) {
			assertEquals(12, size(stream));
		}

		// TAR inside ZIP
		try (DirectoryStream<Input> stream = Wildcards.newDirectoryStream(testScenarios, "tar:(zip:(./data-in/wildcards*.zip)#archive*.tar)#file*.txt")) {
			assertEquals(8, size(stream));
		}

		// ZIP inside TAR
		{
			List<String> contents = new ArrayList<>(4);
			try (DirectoryStream<Input> stream = Wildcards.newDirectoryStream(testScenarios, "zip:(tar:(./data-in/wildcards*.tar)#archive*.zip)#file*.txt")) {
				for (Input input: stream) {
					try (InputStream is = input.getInputStream()) {
						contents.add(IOUtils.toString(is));
					}
					LOG.debug(input);
				}
			}
			assertEquals(Arrays.asList("AAAAAAAAA", "BBBBBBBBBB", "DDDDDDDDDD", "EEEEEEEEEE"), contents);
		}

		// STDIN
		try (DirectoryStream<Input> stream = Wildcards.newDirectoryStream(contextUrl, "-")) {
			for (Input input: stream) {
				assertEquals("-", input.getAbsolutePath());
				try (InputStream is = input.getInputStream()) {}
			}
		}

		// invoking iterator() after close()
		try (DirectoryStream<Input> stream = Wildcards.newDirectoryStream(testScenarios, "zip:(tar:(./data-in/wildcards*.tar)#archive*.zip)#file*.txt")) {
			stream.close();
			try {
				stream.iterator();
				fail();
			} catch (IllegalStateException ex) {
			}
		}

		// reading from archive entry after the DirectoryStream was closed 
		try (DirectoryStream<Input> stream = Wildcards.newDirectoryStream(testScenarios, "zip:(tar:(./data-in/wildcards*.tar)#archive*.zip)#file*.txt")) {
			Iterator<Input> it = stream.iterator();
			assertTrue(it.hasNext());
			stream.close();
			Input input = it.next();
			try (InputStream is = input.getInputStream()) {
				try {
					IOUtils.toString(is);
					fail();
				} catch (IOException ioe) {
					// closing the DirectoryStream should close the underlying InputStream
				}
			}
		}

		{
			// spaces and national characters in archive entry name
			String suffix = "%C5%BElu%C5%A5ou%C4%8Dk%C3%BD%20k%C5%AF%C5%88.txt"; // zlutoucky kun.txt
			
			// test spaces and national characters in entry name
			// TAR
			try (DirectoryStream<Input> stream = Wildcards.newDirectoryStream(testScenarios, "tar:(./data-in/UDR/CLO-6584/*.tar)#*.txt")) {
				for (Input input: stream) {
					assertTrue(input.getAbsolutePath().endsWith(suffix));
					try (InputStream is = input.getInputStream()) {
						String s = IOUtils.toString(is); // read all the bytes
						assertEquals(1, s.length());
					}
				}
			}
			
			// TAR.GZ
			try (DirectoryStream<Input> stream = Wildcards.newDirectoryStream(testScenarios, "tgz:(./data-in/UDR/CLO-6584/*.tar.gz)#*.txt")) {
				for (Input input: stream) {
					assertTrue(input.getAbsolutePath().endsWith(suffix));
					try (InputStream is = input.getInputStream()) {
						String s = IOUtils.toString(is); // read all the bytes
						assertEquals(1, s.length());
					}
				}
			}

			// ZIP
			try (DirectoryStream<Input> stream = Wildcards.newDirectoryStream(testScenarios, "zip:(./data-in/UDR/CLO-6584/*.zip)#*.txt")) {
				for (Input input: stream) {
					assertTrue(input.getAbsolutePath().endsWith(suffix));
					try (InputStream is = input.getInputStream()) {
						String s = IOUtils.toString(is); // read all the bytes
						assertEquals(1, s.length());
					}
				}
			}
		}
		
		// special characters: dollar sign
		try (DirectoryStream<Input> stream = Wildcards.newDirectoryStream(testScenarios, "zip:(./data-in/UDR/CLO-6584/escaping/dollar-sign.zip)#*")) {
			int count = 0;
			for (Input input: stream) {
				count++;
				String path = input.getAbsolutePath();
				LOG.debug(path);
				assertFalse(path.contains("$"));
				assertTrue(path.contains("%24"));
				assertTrue(path.contains("/"));
			}
			assertEquals(4, count);
		}
		
	}

	private int size(DirectoryStream<Input> stream) {
		int count = 0;
		for (Input input: stream) {
			count++;
			LOG.debug(input);
		}
		return count;
	}

}
