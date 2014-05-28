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
package org.jetel.util.stream;

import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.zip.Checksum;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.util.Utils;
import net.jpountz.xxhash.XXHashFactory;

import org.jetel.util.bytes.CloverBuffer;

public class CloverDataStream {

	enum DataBlockType {

		RAW_DATA('D'), COMPRESSED('C'), ENCRYPTED('E'), ENCRYPTED_COMPRESSED('F'), INDEX('I'), INVALID(' ');

		private byte id;

		DataBlockType(int id) {
			this.id = (byte) id;
		}

		public byte getId() {
			return id;
		}

		public static final DataBlockType get(byte value) {
			switch (value) {
			case 'D':
				return RAW_DATA;
			case 'C':
				return COMPRESSED;
			case 'E':
				return ENCRYPTED;
			case 'F':
				return ENCRYPTED_COMPRESSED;
			case 'I':
				return INDEX;
			default:
				return INVALID;
			}
		}
	}

	static abstract class Compressor {
		
		abstract int compress(byte[] source, int sourceOffset, int length, byte[] target, int targetOffset);
		abstract int maxCompressedLength(int sourceLength);
		
	}

	static abstract class Decompressor {
		abstract int decompress(byte[] source, int sourceOffset, int sourceLength, byte[] target, int targetOffset, int rawDataLength);
	}

	
	public static class CompressorLZ4 extends Compressor{

		private final LZ4Compressor compressor;

		public CompressorLZ4() {
			this.compressor = LZ4Factory.fastestInstance().fastCompressor();
		}
		
		@Override
		final int compress(byte[] source, int sourceOffset, int length, byte[] target, int targetOffset) {
			return compressor.compress(source, sourceOffset, length, target, targetOffset);
		}
		
		@Override
		final int maxCompressedLength(int sourceLength){
			return compressor.maxCompressedLength(sourceLength);
		}
	}
	
	public static class DecompressorLZ4 extends Decompressor{

		private final LZ4FastDecompressor decompressor;

		public DecompressorLZ4() {
			this.decompressor = LZ4Factory.fastestInstance().fastDecompressor();
		}
		
		@Override
		final int decompress(byte[] source, int sourceOffset, int sourceLength, byte[] target, int targetOffset, int rawDataLength) {
			return decompressor.decompress(source, sourceOffset, target,  targetOffset, rawDataLength);
		}
		
	}
	
	public static class CompressorGZIP extends Compressor {

		private Deflater compressor;
		
		public CompressorGZIP(){
			this.compressor= new Deflater();
		}
		
		@Override
		final int compress(byte[] source, int sourceOffset, int length, byte[] target, int targetOffset) {
			compressor.setInput(source, sourceOffset, length);
			compressor.finish();
			int size = compressor.deflate(target, targetOffset, target.length-targetOffset);
			if (!compressor.finished()){
				size=-1; // problem - we need bigger target buffer
			}
			compressor.reset();
			return size;
		}
		
		@Override
		final int maxCompressedLength(int sourceLength){
			return sourceLength +
			          ((sourceLength + 7) >> 3) + ((sourceLength + 63) >> 6) + 5;
		}
	}
	
	public static class DecompressorGZIP extends Decompressor {

		private Inflater decompressor;
		
		public DecompressorGZIP(){
			this.decompressor = new Inflater();
		}
		
		@Override
		final int decompress(byte[] source, int sourceOffset, int sourceLength, byte[] target, int targetOffset, int rawDataLength) {
			decompressor.setInput(source, sourceOffset, sourceLength );
			int size;
			try {
				size = decompressor.inflate(target, targetOffset, rawDataLength);
				if (!decompressor.finished()){
					size= -1; // problem
				}
			} catch (DataFormatException e) {
				size= -1;
			}finally{
				decompressor.reset();
			}
			return size;
		}
		
	}
	
	
	/**
	 * the minimum compression ratio to go with compressed instead of full bytes
	 * if block of data after compressions is still quite large compared to full set, we store the full set to save time when reading
	 */
	public static final double MIN_COMPRESS_RATIO = 0.85;
	/**
	 * number of rounds when compression ratio is lower than threshold before we switch to uncompressed mode.
	 * if we get often bad results (low compression) then we switch directly to uncompressed mode to save time during write
	 */
	static final int NO_TEST_ROUNDS = 4; 
	
	/**
	 * This is the identification of Clover data block
	 */
	static final byte[] CLOVER_BLOCK_MAGIC = new byte[] { 'C', 'L', 'V' };
	static final int CLOVER_BLOCK_MAGIC_LENGTH = CLOVER_BLOCK_MAGIC.length;
	static final int CLOVER_BLOCK_HEADER_LENGTH = 
			CLOVER_BLOCK_MAGIC_LENGTH + 1 // token (what type of bloc - compressed/raw/etcc.. - see DataBlockType
			+ 4 // compressed length
			+ 4 // full length
			+ 4 // checksum
			+ 4; // position of the first record in block

	private final static int LONG_SIZE_BYTES = 8;
	
	static final int DEFAULT_BLOCK_INDEX_SIZE = 128;
	static final ByteOrder BUFFER_BYTE_ORDER = ByteOrder.BIG_ENDIAN;

	public final static int findNearestPow2(int size) {
		int value = 1;
		while (value < size) {
			value <<= 1;
		}
		return value;
	}

	public final static boolean testBlockHeader(CloverBuffer buffer) {
		return testBlockHeader(buffer.array(), 0);
	}

	public final static boolean testBlockHeader(ByteBuffer buffer) {
		return testBlockHeader(buffer.array(), 0);
	}

	public final static boolean testBlockHeader(ByteBuffer buffer, DataBlockType type) {
		if (testBlockHeader(buffer.array(), 0) && buffer.get(CloverDataStream.CLOVER_BLOCK_MAGIC_LENGTH) == type.getId())
			return true;
		else
			return false;
	}

	public final static boolean testBlockHeader(byte[] data, int offset) {
		if (data[offset] == CLOVER_BLOCK_MAGIC[0] && data[offset + 1] == CLOVER_BLOCK_MAGIC[1] && data[offset + 2] == CLOVER_BLOCK_MAGIC[2]) {
			return true;
		} else {
			return false;
		}

	}

	public static final class Output extends FilterOutputStream {

		/**
		 * 
		 */
		public static final int DEFAULT_BLOCK_SIZE = 1 << 17; //131072 bytes
		static final int COMPRESSION_LEVEL_BASE = 10;
		static final int MIN_BLOCK_SIZE = 64;
		static final int MAX_BLOCK_SIZE = 1 << (COMPRESSION_LEVEL_BASE + 0x0F);

		static final int DEFAULT_SEED = 0x9747b28c;

		private final Compressor compressor;
		private final Checksum checksum;
		private final CloverBuffer buffer;
		private final CloverBuffer compressedBuffer;
		private long[] blocksIndex;

		private final boolean syncFlush;
		private boolean finished;
		private boolean compress;
		private long position;
		private int firstRecordPosition;
		private int testRound;

		/**
		 * Create a new {@link OutputStream} with configurable block size. Large blocks require more memory at
		 * compression and decompression time but should improve the compression ratio.
		 * 
		 * @param out
		 *            the {@link OutputStream} to feed
		 * @param blockSize
		 *            the maximum number of bytes to try to compress at once, must be >= 64 and <= 32 M
		 * @param compressor
		 *            the {@link LZ4Compressor} instance to use to compress data
		 * @param checksum
		 *            the {@link Checksum} instance to use to check data for integrity.
		 * @param syncFlush
		 *            true if pending data should also be flushed on {@link #flush()}
		 */
		public Output(OutputStream out, int blockSize, int blockIndexSize, Compressor compressor, Checksum checksum,
				boolean syncFlush) {
			super(out);
			this.compressor = compressor;
			this.checksum = checksum;
			this.buffer = CloverBuffer.wrap(new byte[blockSize]);
			this.buffer.order(BUFFER_BYTE_ORDER);
			this.buffer.setRecapacityAllowed(false);
			final int compressedBlockSize = CLOVER_BLOCK_HEADER_LENGTH + compressor.maxCompressedLength(blockSize);
			this.compressedBuffer = CloverBuffer.wrap(new byte[compressedBlockSize]);
			this.compressedBuffer.order(BUFFER_BYTE_ORDER);
			this.compressedBuffer.setRecapacityAllowed(false);
			this.syncFlush = syncFlush;
			finished = false;
			compressedBuffer.put(CLOVER_BLOCK_MAGIC);
			this.position = 0;
			this.firstRecordPosition = -1;
			this.blocksIndex = blockIndexSize > DEFAULT_BLOCK_INDEX_SIZE ? new long[((blockIndexSize >> 1) << 1)] : new long[DEFAULT_BLOCK_INDEX_SIZE];
			this.compress = false; // no compression by default
			this.testRound=0;
		}

		/**
		 * Create a new instance which checks stream integrity using {@link StreamingXXHash32} and doesn't sync flush.
		 * 
		 * @see #LZ4BlockOutputStream(OutputStream, int, LZ4Compressor, Checksum, boolean)
		 * @see StreamingXXHash32#asChecksum()
		 */
		public Output(OutputStream out, int blockSize, Compressor compressor) {
			this(out, blockSize, DEFAULT_BLOCK_INDEX_SIZE, compressor, XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum(), false);
		}

		/**
		 * Create a new instance which compresses with the standard LZ4 compression algorithm.
		 * 
		 * @see #LZ4BlockOutputStream(OutputStream, int, LZ4Compressor)
		 * @see LZ4Factory#fastCompressor()
		 */
		public Output(OutputStream out, int blockSize) {
			this(out, blockSize, new CompressorLZ4());
		}

		/**
		 * Create a new instance which compresses into blocks of DEFAULT_BLOCK_SIZE KB.
		 * 
		 * @see #LZ4BlockOutputStream(OutputStream, int)
		 */
		public Output(OutputStream out) {
			this(out, DEFAULT_BLOCK_SIZE);
		}

		public long getPosition() {
			return position;
		}

		public void setPosition(long position) {
			this.position = position;
		}

		public boolean isCompress() {
			return compress;
		}

		public void setCompress(boolean compress) {
			this.compress = compress;
		}

		private final void ensureNotFinished() {
			if (finished) {
				throw new IllegalStateException("This stream is already closed");
			}
		}

		@Override
		public void write(int b) throws IOException {
			ensureNotFinished();
			if (!buffer.hasRemaining()) {
				flushBufferedData();
			}
			buffer.put((byte) b);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			//Utils.checkRange(b, off, len);
			ensureNotFinished();

			while (buffer.remaining() < len) {
				final int l = buffer.remaining();
				buffer.put(b, off, l);
				flushBufferedData();
				off += l;
				len -= l;
			}
			buffer.put(b, off, len);
		}

		public void write(CloverBuffer inbuffer) throws IOException {
			ensureNotFinished();

			while (buffer.remaining() < inbuffer.remaining()) {
				final int savelimit = inbuffer.limit();
				inbuffer.limit(inbuffer.position() + buffer.remaining());
				buffer.put(inbuffer);
				inbuffer.limit(savelimit);
				flushBufferedData();
			}
			buffer.put(inbuffer);
		}

		public void write(ByteBuffer inbuffer) throws IOException {
			ensureNotFinished();
			while (buffer.remaining() < inbuffer.remaining()) {
				final int savelimit = inbuffer.limit();
				inbuffer.limit(inbuffer.position() + buffer.remaining());
				buffer.put(inbuffer);
				inbuffer.limit(savelimit);
				flushBufferedData();
			}
			buffer.put(inbuffer);
		}

		@Override
		public void write(byte[] b) throws IOException {
			ensureNotFinished();
			write(b, 0, b.length);
		}
		
		public final void markRecordStart(){
			if (firstRecordPosition < 0)
				firstRecordPosition = buffer.position();
		}

		@Override
		public void close() throws IOException {
			writeIndexData(); // this also marks the end of data
			if (!finished) {
				finish();
			}
			if (out != null) {
				out.close();
				out = null;
			}
		}

		private void flushBufferedData() throws IOException {
			if (buffer.position() == 0)
				return;
			// store index of new block which will be added (but only if it contains beginning of record
			if (firstRecordPosition >= 0)
				storeBlockIndex();

			buffer.flip();

			final int rawlength = buffer.remaining();
			if (compress) {
				compressedBuffer.clear();
				final int compressedLength = compressor.compress(buffer.array(), 0, rawlength, compressedBuffer.array(), CLOVER_BLOCK_HEADER_LENGTH);
				if (compressedLength==-1){
					throw new IOException("Error when compressing datablock.");
				}
				
				double ratio=((double)compressedLength)/rawlength;
				//DEBUG
				//System.err.println("compress ratio= "+ratio);
				if (ratio > MIN_COMPRESS_RATIO){
					if ((testRound++)>NO_TEST_ROUNDS){
						compress=false; // we are forcing switch off of compression
					}
					
				}
				
				if (compressedLength < rawlength) {
					fillBlockHeader(compressedBuffer,DataBlockType.COMPRESSED,compressedLength,rawlength,0,firstRecordPosition);
					// write header+data
					out.write(compressedBuffer.array(), 0, CLOVER_BLOCK_HEADER_LENGTH + compressedLength);
					position += CLOVER_BLOCK_HEADER_LENGTH + compressedLength;
					buffer.clear();
					firstRecordPosition = -1; // reset
					return;

				}
			}
			fillBlockHeader(compressedBuffer,DataBlockType.RAW_DATA,rawlength,rawlength,0,firstRecordPosition);
			// write header
			out.write(compressedBuffer.array(), 0, CLOVER_BLOCK_HEADER_LENGTH);
			// write data
			out.write(buffer.array(), 0, rawlength);
			position += CLOVER_BLOCK_HEADER_LENGTH + rawlength;

			buffer.clear();
			firstRecordPosition = -1; // reset
		}

		private final void fillBlockHeader(CloverBuffer buffer,DataBlockType type,int rawLength,int compressedLength,int checksum,int firstRecPos){
			buffer.position(0).put(CLOVER_BLOCK_MAGIC);
			buffer.put(type.getId());
			buffer.putInt(compressedLength);
			buffer.putInt(rawLength);
			buffer.putInt(checksum);
			buffer.putInt(firstRecPos);
		}
		
		/**
		 * Flush this compressed {@link OutputStream}.
		 * 
		 * If the stream has been created with <code>syncFlush=true</code>, pending data will be compressed and appended
		 * to the underlying {@link OutputStream} before calling {@link OutputStream#flush()} on the underlying stream.
		 * Otherwise, this method just flushes the underlying stream, so pending data might not be available for reading
		 * until {@link #finish()} or {@link #close()} is called.
		 */
		@Override
		public void flush() throws IOException {
			if (syncFlush) {
				flushBufferedData();
			}
			out.flush();
		}

		/**
		 * Same as {@link #close()} except that it doesn't close the underlying stream. This can be useful if you want
		 * to keep on using the underlying stream.
		 */
		public void finish() throws IOException {
			finished = true;
			out.flush();
		}

		/**
		 * Writes INDEX block to the stream. This block should be very last. It contains index of all/most previous data
		 * (raw & compressed) blocks. The header is written twice - as the first & very last set of bytes of this block
		 * - so it can be read from the end of the data stream. The size of the index may vary.
		 * 
		 * @throws IOException
		 */
		public void writeIndexData() throws IOException {
			ensureNotFinished();
			flushBufferedData();
			try {
				for (long value : blocksIndex) {
					if (value > 0) {
						buffer.putLong(value);
					}
				}
			} catch (BufferOverflowException ex) {
				throw new IOException("Can't store index data - internal buffer too small");
			}
			buffer.flip();
			final int size = buffer.remaining();
			checksum.reset();
			checksum.update(buffer.array(), 0, size);
			final int check = (int) checksum.getValue();
			fillBlockHeader(compressedBuffer, DataBlockType.INDEX,size+CLOVER_BLOCK_HEADER_LENGTH, size+CLOVER_BLOCK_HEADER_LENGTH, check, 0);
			position += CLOVER_BLOCK_HEADER_LENGTH + buffer.remaining();
			out.write(compressedBuffer.array(), 0, CLOVER_BLOCK_HEADER_LENGTH);
			out.write(buffer.array(), 0, buffer.remaining());

			// write the header at the end again so it can be easily read from back
			// the size this time does not contain the block HEADER length
			compressedBuffer.putInt(CLOVER_BLOCK_MAGIC_LENGTH + 1, size);
			compressedBuffer.putInt(CLOVER_BLOCK_MAGIC_LENGTH + 5, size);
			out.write(compressedBuffer.array(), 0, CLOVER_BLOCK_HEADER_LENGTH);
			out.flush();
		}

		private final void storeBlockIndex() throws IOException {
			if (blocksIndex.length > 0) {
				if (!addBlockIndex(position)) {
					compactBlockIndex();
					if (!addBlockIndex(position)) {
						throw new IOException("Can't store position in index - no space left");
					}
				}
			}
		}

		private final boolean addBlockIndex(long position) {
			for (int i = 0; i < blocksIndex.length; i++) {
				if (blocksIndex[i] == 0) {
					blocksIndex[i] = position;
					return true;
				}

			}
			return false;
		}

		private final void compactBlockIndex() {
			for (int i = 1; i < blocksIndex.length; i += 2) {
				blocksIndex[i >> 1] = blocksIndex[i];
				blocksIndex[i] = 0;
			}
		}

		/**
		 * Seeks the channel to position where appending data can start.
		 * It first reads the stored index in existing file (if index present) and
		 * then positions the channel so appended data rewrites the index.
		 * 
		 * @param channel
		 * @throws IOException
		 */
		public void seekToAppend(SeekableByteChannel channel) throws IOException {
			ByteBuffer buffer = ByteBuffer.allocate(CLOVER_BLOCK_HEADER_LENGTH);
			channel.position(channel.size() - CLOVER_BLOCK_HEADER_LENGTH);
			channel.read(buffer);
			buffer.flip();
			// test that it is our Index block
			if (testBlockHeader(buffer, DataBlockType.INDEX)) {
				int size = buffer.getInt(CLOVER_BLOCK_MAGIC_LENGTH + 1);
				int check = buffer.getInt(CLOVER_BLOCK_MAGIC_LENGTH + 9);
				// reallocate bytebuffer
				buffer = ByteBuffer.wrap(new byte[CLOVER_BLOCK_HEADER_LENGTH + size]);
				channel.position(channel.size() - CLOVER_BLOCK_HEADER_LENGTH - size);
				channel.read(buffer);
				buffer.flip();
				// validate checksum
				checksum.reset();
				checksum.update(buffer.array(), 0, size);
				if (check != (int) checksum.getValue()) {
					throw new IOException("Invalid checksum when reading index data !!! Possibly corrupted data file.");
				}
				// we got correct checksum, so populate our index data
				int storedindexsize = size / 8;
				if (blocksIndex.length < storedindexsize) {
					blocksIndex = new long[findNearestPow2(size)];
				}
				buffer.limit(size);
				int index = 0;
				while (buffer.hasRemaining() && (index < blocksIndex.length)) {
					blocksIndex[index++] = buffer.getLong();
				}
				// seek to the position of index header as we are going to replace it during append
				position = channel.size() - CLOVER_BLOCK_HEADER_LENGTH - size - CLOVER_BLOCK_HEADER_LENGTH;
			} else {
				// seek to the end
				position = channel.size() - 1;
			}
			channel.position(position);
		}

	}

	public final static class Input extends FilterInputStream {

		static final int DEFAULT_SEED = 0x9747b28c;

		private final Decompressor decompressor;
		private final Checksum checksum;
		private CloverBuffer buffer;
		private CloverBuffer compressedBuffer;
		private long[] blocksIndex;

		private long position;
		private int firstRecordPosition;
		private boolean hasIndex;
		
		private SeekableByteChannel seekableChannel;

		/**
		 * Create a new {@link OutputStream} with configurable block size. Large blocks require more memory at
		 * compression and decompression time but should improve the compression ratio.
		 * 
		 * @param out
		 *            the {@link OutputStream} to feed
		 * @param blockSize
		 *            the maximum number of bytes to try to compress at once, must be >= 64 and <= 32 M
		 * @param decompressor
		 *            the {@link LZ4Decompressor} instance to use to compress data
		 */
		public Input(InputStream in, Decompressor decompressor, Checksum checksum) {
			super(in);
			this.decompressor = decompressor;
			this.checksum = checksum;
			this.position = 0;
			this.firstRecordPosition = -1;
			this.blocksIndex = new long[0]; // initally set to zero length
			this.buffer = CloverBuffer.wrap(new byte[CLOVER_BLOCK_HEADER_LENGTH]);
			this.buffer.order(BUFFER_BYTE_ORDER);
			this.buffer.flip(); // set to empty (no data)
		}

		/**
		 * Create a new instance which checks stream integrity using {@link StreamingXXHash32} and doesn't sync flush.
		 * 
		 * @see #LZ4BlockOutputStream(OutputStream, int, LZ4Compressor, Checksum, boolean)
		 * @see StreamingXXHash32#asChecksum()
		 */
		public Input(InputStream in, Decompressor decompressor) {
			this(in, decompressor, XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum());
		}

		public Input(InputStream in) {
			this(in, new DecompressorLZ4());
		}

		public Input(SeekableByteChannel channel, Decompressor decompressor){
			this(Channels.newInputStream(channel),decompressor);
			this.seekableChannel=channel;
		}
		
		public long getPosition() {
			return position;
		}

		public void setPosition(long position) {
			this.position = position;
		}

		@Override
		public int read() throws IOException {
			if (!buffer.hasRemaining()) {
				if (!readDataBlock())
					return -1; // end of stream
			}
			return buffer.get() & 0xFF;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			Utils.checkRange(b, off, len);
			int count = 0;
			while (buffer.remaining() < len) {
				final int l = buffer.remaining();
				buffer.get(b, off, l);
				count += l;
				off += l;
				len -= l;
				if (!readDataBlock())
					return -1; //end of stream;

			}
			buffer.get(b, off, len);
			count += len;
			return count;
		}

		public int read(CloverBuffer inbuffer) throws IOException {
			final int pos=inbuffer.position();
			while (buffer.remaining() < inbuffer.remaining()) {
				inbuffer.put(buffer);
				if (!readDataBlock())
					return -1;
			}
			final int savelimit = buffer.limit();
			buffer.limit(buffer.position() + inbuffer.remaining());
			inbuffer.put(buffer);
			buffer.limit(savelimit);
			return inbuffer.position()-pos;
		}

		public int read(ByteBuffer inbuffer) throws IOException {
			final int pos=inbuffer.position();
			while (buffer.remaining() < inbuffer.remaining()) {
				inbuffer.put(buffer.array(), buffer.position(), buffer.remaining());
				if (!readDataBlock())
					return -1;
			}
			final int savelimit = buffer.limit();
			buffer.limit(buffer.position() + inbuffer.remaining());
			inbuffer.put(buffer.array(), buffer.position(), buffer.remaining());
			buffer.limit(savelimit);
			return inbuffer.position()-pos;
		}

		@Override
		public void close() throws IOException {
			if (in != null) {
				in.close();
				in = null;
			}
		}

		private boolean readDataBlock() throws IOException {
			buffer.clear();
			// store index of new block which will be added (but only if it contains beginning of record
			// firstRecordPosition
			final int readin=StreamUtils.readBlocking(in, buffer.array(), 0, CLOVER_BLOCK_HEADER_LENGTH);
			if (readin==-1) return false;
			if (readin!= CLOVER_BLOCK_HEADER_LENGTH || !testBlockHeader(buffer)) {
				throw new IOException("Missing block header. Probably corrupted data !");
			}
			boolean compressed = false;
			switch (DataBlockType.get(buffer.get(CLOVER_BLOCK_MAGIC_LENGTH))) {
			case COMPRESSED:
				compressed = true;
				break;
			case RAW_DATA:
				break;
			case INDEX:
				// return, no more data, the index block is always the last
				return false;
			}

			int rawLength = buffer.position(CLOVER_BLOCK_MAGIC_LENGTH + 1).getInt();
			int compressedLength = buffer.getInt(); 
			int checksum = buffer.getInt(); // not used currently (only in index block)
			firstRecordPosition = buffer.getInt();
			
			buffer.clear();
			if (buffer.capacity() < rawLength) {
				buffer = CloverBuffer.wrap(new byte[findNearestPow2(rawLength)]);
				buffer.order(BUFFER_BYTE_ORDER);
			}
			if (compressed) {
				if (compressedBuffer == null || compressedBuffer.capacity() < compressedLength) {
					compressedBuffer = CloverBuffer.wrap(new byte[findNearestPow2(compressedLength)]);
					compressedBuffer.order(BUFFER_BYTE_ORDER);
				}
				if (StreamUtils.readBlocking(in, compressedBuffer.array(), 0, compressedLength) != compressedLength) {
					throw new IOException("Unexpected end of file");
				}
				decompressor.decompress(compressedBuffer.array(), 0, compressedLength , buffer.array(), 0, rawLength);
			} else {
				if (StreamUtils.readBlocking(in, buffer.array(), 0, rawLength) != rawLength) {
					throw new IOException("Unexpected end of file");
				}
			}
			buffer.position(0);
			buffer.limit(rawLength);
			return true;
		}

		private final long findNearestBlockIndex(long startAt) {
			int pos = Arrays.binarySearch(blocksIndex, startAt);
			if (pos < 0) {
				return blocksIndex[~pos];
			} else if (pos < blocksIndex.length) {
				return blocksIndex[pos];
			}
			return -1;
		}

		public void readIndexData() throws IOException {
			if (this.seekableChannel==null){
				throw new IOException("Not a seekable channel/data stream.");
			}
			position = seekableChannel.position();
			ByteBuffer buffer = ByteBuffer.allocate(CLOVER_BLOCK_HEADER_LENGTH);
			seekableChannel.position(seekableChannel.size() - CLOVER_BLOCK_HEADER_LENGTH);
			seekableChannel.read(buffer);
			buffer.flip();
			// test that it is our Index block
			if (testBlockHeader(buffer, DataBlockType.INDEX)) {
				int size = buffer.getInt(CLOVER_BLOCK_HEADER_LENGTH + 1);
				int check = buffer.getInt(CLOVER_BLOCK_HEADER_LENGTH + 9);
				// reallocate bytebuffer
				buffer = ByteBuffer.wrap(new byte[CLOVER_BLOCK_HEADER_LENGTH + size]);
				seekableChannel.position(seekableChannel.size() - CLOVER_BLOCK_HEADER_LENGTH - size);
				seekableChannel.read(buffer);
				buffer.flip();
				// validate checksum
				checksum.reset();
				checksum.update(buffer.array(), 0, size);
				if (check != (int) checksum.getValue()) {
					throw new IOException("Invalid checksum when reading index data !!! Possibly corrupted data file.");
				}
				// we got correct checksum, so populate our index data
				int storedindexsize = size / LONG_SIZE_BYTES;
				blocksIndex = new long[storedindexsize];
				buffer.limit(size);
				int index = 0;
				while (buffer.hasRemaining() && (index < blocksIndex.length)) {
					blocksIndex[index++] = buffer.getLong();
				}
				hasIndex = true;
			} else {
				hasIndex = false;
			}
			// seek back to where we started
			seekableChannel.position(position);
		}
		
		public long size() throws IOException{
			return (seekableChannel!=null) ? seekableChannel.size() : -1;
		}
		
		public long seekTo(long position) throws IOException{
			if (!hasIndex) return -1;
			long blockPosition = findNearestBlockIndex(position);
			if (blockPosition==-1) return -1;
			
			seekableChannel.position(blockPosition);
			if(!readDataBlock()) throw new IOException("Unable to seek.");
			if (firstRecordPosition>=0){
				buffer.position(firstRecordPosition);
				return blockPosition+firstRecordPosition;
			}else{
				return -1;
			}
		}
		
	}

}