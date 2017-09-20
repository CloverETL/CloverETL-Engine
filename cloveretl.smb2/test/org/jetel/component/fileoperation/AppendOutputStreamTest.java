package org.jetel.component.fileoperation;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.lf5.util.StreamUtils;
import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.SimpleParameters.DeleteParameters;
import org.jetel.test.CloverTestCase;
import org.jetel.util.file.FileUtils;
import org.jetel.util.stream.CloseOnceOutputStream;

public class AppendOutputStreamTest extends CloverTestCase {

	private static final String BASE_URL = "smb2://Administrator:sem%40f0r@virt-test/smbtest/append/";

	private static final FileManager FILE_MANAGER = FileManager.getInstance();
	
	private static final int MB = 1024 * 1024;
	
	private URI dir;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		dir = URI.create(BASE_URL + System.currentTimeMillis() + "/");
		FILE_MANAGER.create(CloverURI.createSingleURI(dir), new CreateParameters().setMakeParents(true));
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		FILE_MANAGER.delete(CloverURI.createSingleURI(dir), new DeleteParameters().setRecursive(true));
		dir = null;
	}

	public void testAppend() throws Exception {
		String url = dir.resolve("testAppend.tmp").toString();
		
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
	}
	
	public void testAppendPerformance() throws Exception {
		final String url = dir.resolve("testAppendPerformance.tmp").toString();
		
		ExecutorService service = Executors.newSingleThreadExecutor();
		try {
			Callable<Void> callable = new Callable<Void>() {

				@Override
				public Void call() throws Exception {
					try (OutputStream os = getOutputStream(url, true)) {
						for (int i = 0; i < 100000; i++) {
							os.write(1);
						}
					}
					return null;
				}
				
			};
			Future<Void> future = service.submit(callable);
			
			future.get(3, TimeUnit.SECONDS);
		} catch (TimeoutException ex) {
			fail("Append timed out"); // too slow, probably no buffering
		} finally {
			service.shutdownNow();
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
