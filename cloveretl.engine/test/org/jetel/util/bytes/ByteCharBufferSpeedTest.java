package org.jetel.util.bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.jetel.test.CloverTestCase;
import org.jetel.util.bytes.ByteCharBuffer;

/**
 * This class shows performance of ByteCharBuffer vs. InputStreamReader
 * 
 * @author pnajvar
 *
 */
public class ByteCharBufferSpeedTest extends CloverTestCase {

	/*
	 * Path to a big file
	 * Either edit and compile or set a system property
	 */
	String bigFile = null;
	/*
	 * The file must be valid for the following charset
	 */
	String charset = "utf-8";
	/*
	 * Repeat
	 */
	int testCount = 2;
	
	public void testSpeed() throws Exception {
		if (bigFile == null) {
			bigFile = System.getProperty("bigFile");
		}
		if (bigFile == null || ! new File(bigFile).exists()) {
			System.out.println("Please specify path to a big text file via `bigFile` system property.\nExample: -DbigFile=/home/joe/bigdata/file.txt -DbigFileCharset=utf-8\nCurrent: " + bigFile + "\nCharset: " + charset.toUpperCase());
			return;
		}
		
		if (System.getProperty("bigFileCharset") != null) {
			charset = System.getProperty("bigFileCharset");
		}
		
		System.out.println("Big file: " + bigFile);
		System.out.println("Charset : " + charset.toUpperCase());
		double sum1 = 0;
		double sum2 = 0;
		for(int i = 0; i < testCount; i++) {
			sum1 += doInputStreamReader();
			sum2 += doByteCharBuffer();
		}
		System.out.println("Results (avg):");
		System.out.println("  InputStreamReader(buffered): " + (sum1/(float)testCount) + " s");
		System.out.println("  ByteCharBuffer:              " + (sum2/(float)testCount) + " s");
	}
	
	private double doByteCharBuffer() throws Exception {
		
		ByteCharBuffer b = new ByteCharBuffer();
		b.init(500000, 500000);
		b.setSource(new FileInputStream(bigFile).getChannel(), "utf-8");

		long s = System.currentTimeMillis();
		System.out.print("-- Running ByteCharBuffer... ");
		int c;
		while ( (c = b.get()) != -1) {
//			System.out.print((char)c);
		}
		
		double el = (System.currentTimeMillis() - s)/1000.0;
		System.out.println("finished in " + el + " s --");
		return el;
		
	}

	
	private double doInputStreamReader() throws Exception {
		
		
		BufferedReader r = new BufferedReader(new InputStreamReader(new FileInputStream(bigFile), "utf-8"));
		
		
		long s = System.currentTimeMillis();
		System.out.print("-- Running InputStreamReader(buffered)... ");
		int c;
		while ( (c = r.read()) != -1) {
//			System.out.print((char)c);
		}
		
		double el = (System.currentTimeMillis() - s)/1000.0;
		System.out.println("finished in " + el + " s --");
		return el;
		
	}
	
	
}
