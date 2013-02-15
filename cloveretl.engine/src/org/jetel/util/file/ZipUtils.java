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
package org.jetel.util.file;

import java.io.ByteArrayOutputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.jetel.data.Defaults;

/**
 * Class for gzip compression/decompression. It creates compressor/decompressor for any thread
 * which demands compression/decompression. Each thread has only one compressor/decompressor.
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 12/01/06  
 */
public class ZipUtils {
	/** instance associating compressers with threads */
	private static ThreadLocal<Compresser> compressors = new ThreadLocal<Compresser>();
	/** instance associating decompressers with threads */
	private static ThreadLocal<Decompresser> decompressors = new ThreadLocal<Decompresser>();

	/**
	 * Gets compresser associated with calling thread.
	 * @return
	 */
	private static Compresser getCompresser() {
		Compresser compresser = compressors.get();
		if (compresser == null) {
			compresser = new Compresser(Defaults.Record.DEFAULT_COMPRESSION_LEVEL);
			compressors.set(compresser);
		}
		return compresser;
	}
	
	/**
	 * Gets decompresser associated with calling thread.
	 * @return
	 */
	private static Decompresser getDecompresser() {
		Decompresser decompresser = decompressors.get();
		if (decompresser == null) {
			decompresser = new Decompresser();
			decompressors.set(decompresser);
		}
		return decompresser;
	}

	private static void free(Compresser compresser) {		
	}
	
	private static void free(Decompresser decompresser) {		
	}

	/**
	 * Compresses data.
	 * @param input Data to be compressed.
	 * @return Compressed data
	 */
	public static byte[] compress(byte[] input) {
		if (input == null) {
			return null;
		}

		Compresser compresser = getCompresser();
		byte[] output = compresser.compress(input);
		compresser.release();
		return output;
	}

	/**
	 * Compresses data. Throws runtime error when size of compressed data doesn't match expected size specified
	 * by parameter. Faster than similar method without <code>outSize</code> parameter.
	 * @param input Data to be compressed.
	 * @param outSize Expected size of compressed data
	 * @return Compressed data
	 */
	public static byte[] compress(byte[] input, int outSize) {		
		if (input == null) {
			return null;
		}

		Compresser compresser = getCompresser();
		byte[] output = compresser.compress(input, outSize);
		compresser.release();
		return output;
	}
	
	/**
	 * Decompresses data.
	 * @param input Data to be decompressed.
	 * @return Decompressed data
	 * @throws DataFormatException
	 */
	public static byte[] tryDecompress(byte[] input) throws DataFormatException {
		if (input == null) {
			return null;
		}

		Decompresser decompresser = getDecompresser();
		byte[] output = decompresser.decompress(input);
		decompresser.release();
		return output;
	}
	
	/**
	 * Decompresses data. Throws runtime error when size of decompressed data doesn't match expected size specified
	 * by parameter.  Faster than similar method without <code>outSize</code> parameter.
	 * @param input Data to be decompressed.
	 * @param outSize Expected size of decompressed data.
	 * @return Decompressed data
	 */
	public static byte[] tryDecompress(byte[] input, int outSize) throws DataFormatException {
		if (input == null) {
			return null;
		}

		Decompresser decompresser = getDecompresser();
		byte[] output = decompresser.decompress(input, outSize);
		decompresser.release();
		return output;
	}
	
	/**
	 * Decompresses data. Throws runtime exception for malformed input.
	 * @param input Data to be decompressed.
	 * @return Decompressed data
	 */
	public static byte[] decompress(byte[] input) {
		if (input == null) {
			return null;
		}

		Decompresser decompresser = getDecompresser();
		byte[] output;
		try {
			output = decompresser.decompress(input);
		} catch (DataFormatException e) {
			throw new RuntimeException("Unable to decompress data", e);
		}
		decompresser.release();
		return output;
	}
	
	/**
	 * Decompresses data. Throws runtime exception when size of decompressed data doesn't match expected size specified
	 * by parameter.  Faster than similar method without <code>outSize</code> parameter. Throws runtime exception for malformed input.
	 * @param input Data to be decompressed.
	 * @param outSize Expected size of decompressed data.
	 * @return Decompressed data
	 */
	public static byte[] decompress(byte[] input, int outSize) {
		if (input == null) {
			return null;
		}

		Decompresser decompresser = getDecompresser();
		byte[] output;
		try {
			output = decompresser.decompress(input, outSize);
		} catch (DataFormatException e) {
			throw new RuntimeException("Unable to decompress data", e);
		}
		decompresser.release();
		return output;
	}
	
	private static class Compresser {
		private static final int COMPRESS_BUFFER_SIZE = 4096;

		private Deflater deflater;
		private ByteArrayOutputStream outStream;
		private byte[] outBuf;

		public Compresser(int level) {
			deflater = new Deflater();
			deflater.setLevel(level);
			outBuf = null;
			outStream = null;
		}

		public byte[] compress(byte[] input) {
			if (outBuf == null) {
				outBuf = new byte[COMPRESS_BUFFER_SIZE];
				outStream = new ByteArrayOutputStream(COMPRESS_BUFFER_SIZE);				
			}
			deflater.setInput(input);
			deflater.finish();
			do {
				int clen = deflater.deflate(outBuf);
				outStream.write(outBuf, 0, clen);
			} while (!deflater.finished());
			byte[] retval = outStream.toByteArray();
			outStream.reset();
			deflater.reset();
			return retval;
		}

		public byte[] compress(byte[] input, int outSize) {
			deflater.setInput(input);
			deflater.finish();
			byte[] output = new byte[outSize];
			if (deflater.deflate(output) != outSize || !deflater.finished()) {
				throw new RuntimeException("Size of decompressed data doesn't match expected size");
			}
			deflater.reset();
			return output;
		}
		
		public void release() {
			free(this);
		}
	}

	private static class Decompresser {
		private static final int DECOMPRESS_BUFFER_SIZE = 4096;

		private Inflater inflater;
		private ByteArrayOutputStream outStream;
		private byte[] outBuf;

		public Decompresser() {
			inflater = new Inflater();
			outBuf = new byte[DECOMPRESS_BUFFER_SIZE];
			outStream = new ByteArrayOutputStream(DECOMPRESS_BUFFER_SIZE);
		}

		public byte[] decompress(byte[] input) throws DataFormatException {
			if (outBuf == null) {
				outBuf = new byte[DECOMPRESS_BUFFER_SIZE];
				outStream = new ByteArrayOutputStream(DECOMPRESS_BUFFER_SIZE);				
			}
			inflater.setInput(input);
			do {
				int clen = inflater.inflate(outBuf);
				outStream.write(outBuf, 0, clen);
			} while (!inflater.finished());
			byte[] retval = outStream.toByteArray();
			outStream.reset();
			inflater.reset();
			return retval;
		}

		public byte[] decompress(byte[] input, int outSize) throws DataFormatException {
			inflater.setInput(input);
			byte[] output = new byte[outSize];
			if (inflater.inflate(output) != outSize || !inflater.finished()) {
				throw new RuntimeException("Size of decompressed data doesn't match expected size");
			}
			inflater.reset();
			return output;
		}

		public void release() {
			free(this);
		}
	}

}
