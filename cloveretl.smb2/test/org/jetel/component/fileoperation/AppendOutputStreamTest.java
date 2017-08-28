package org.jetel.component.fileoperation;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.log4j.lf5.util.StreamUtils;
import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.test.CloverTestCase;
import org.jetel.util.file.FileUtils;
import org.jetel.util.stream.CloseOnceOutputStream;

public class AppendOutputStreamTest extends CloverTestCase {

	private static final String BASE_URL = "smb2://Administrator:sem%40f0r@virt-test/smbtest/append/";

	private static final FileManager FILE_MANAGER = FileManager.getInstance();
	
	private static final int MB = 1024 * 1024;

	public void testAppend() throws Exception {
		String filename = String.format("%d.tmp", System.currentTimeMillis());
		String dir = BASE_URL;
		String url = dir + filename;
		
		try {
			FILE_MANAGER.create(CloverURI.createURI(dir), new CreateParameters().setMakeParents(true));
			int headerSize = 4096;
			int appendSize = 16 * MB;

			try (OutputStream os = getOutputStream(url, false)) {
				assertTrue(os instanceof CloseOnceOutputStream);
				byte[] bytes = new byte[headerSize];
				Arrays.fill(bytes, (byte) 1); 
				os.write(bytes);
			}

			try (OutputStream os = getOutputStream(url, true)) {
				assertTrue(os instanceof CloseOnceOutputStream);
				byte[] bytes = new byte[appendSize];
				Arrays.fill(bytes, (byte) 2); 
				os.write(bytes);
			}
			
			try (InputStream is = getInputStream(url);
				ByteCountingOutputStream os = new ByteCountingOutputStream()) {
				StreamUtils.copy(is, os);
				assertEquals(os.getCounter(), headerSize + appendSize); // check the number of bytes
			} 
		} finally {
			FILE_MANAGER.delete(CloverURI.createURI(url));
		}
		
	}
	
	private InputStream getInputStream(String url) throws IOException {
		return FileUtils.getInputStream(null, url);
	}

	private OutputStream getOutputStream(String url, boolean append) throws IOException {
		return FileUtils.getOutputStream(null, url, append, 0);
	}

	private static class ByteCountingOutputStream extends OutputStream {
		
		private long counter = 0;

		@Override
		public void write(int b) throws IOException {
			if (counter < 4096) {
				assertEquals(b, 1);
			} else {
				assertEquals(b, 2);
			}
			counter++;
		}

		public long getCounter() {
			return counter;
		}
				
	}

}
