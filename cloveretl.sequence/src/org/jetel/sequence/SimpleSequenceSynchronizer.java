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
package org.jetel.sequence;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.util.file.FileUtils;

/**
 * Wrapper class for I/O of persisted sequences.
 * Allows safe sharing of the same sequence file by multiple threads and even processes (JVMs) on the same machine.
 * 
 * @author salamonp (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 21. 11. 2014
 */
public class SimpleSequenceSynchronizer {

	/** System lock for the case when someone runs multiple separate JVMs */ 
	private FileLock lock;
	
	/** JVM Lock for thread synchronization (inside the same JVM) */
	private final static Object READ_WRITE_LOCK = new Object();
	
	private FileChannel io;
	private ByteBuffer buffer;
	
	/** Persisted file */
	private File javaFile;
	
	/** Absolute path of the persisted file. */
	private String absoluteFilePath;
	
	private static final Log logger = LogFactory.getLog(SimpleSequenceSynchronizer.class);
    private static final int DATA_SIZE = 8; //how many bytes occupy serialized value in file
    private static final String ACCESS_MODE="rwd";

	private SimpleSequenceSynchronizer(File javaFile, String absolutePath) {
		this.javaFile = javaFile;
		this.absoluteFilePath = absolutePath;
		buffer = ByteBuffer.allocateDirect(DATA_SIZE);
	}
	
	/**
	 * Opens file and obtains an exclusive system lock.
	 * Should always be in synchronized(READ_WRITE_LOCK) and in try finally with closeFile())
	 * @throws IOException
	 */
	private void openFile() throws IOException {
		int limit = 10;
		try {
			io = new RandomAccessFile(absoluteFilePath, ACCESS_MODE).getChannel();
			lock = io.tryLock();

			while (lock == null && limit > 0) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					throw new JetelRuntimeException(e);
				}
				lock = io.tryLock();
			}
		} catch (Exception e) {
			closeFile();
			throw e;
		}

		if (limit == 0) {
			throw new JetelRuntimeException("Can't obtain file lock for sequence file " + absoluteFilePath);
		}

	}
	
	/**
	 * Closes file, releases the exlusive system lock.
	 * @throws IOException
	 */
	private void closeFile() throws IOException {
		if (lock != null) {
			lock.release();
			lock = null;
		}
		if (io != null) {
			io.close();
			io = null;
		}
	}

	/**
	 * "Atomic" get and set operation. Stores incremented value and returns start of new sequence range.
	 * Use this method to advance sequence forward.
	 * @param step
	 * @param numCachedValues
	 * @return
	 * @throws IOException
	 */
	public long getAndSet(long step, long numCachedValues) throws IOException {
		long increment;
		if (numCachedValues <= 0) {
			// backwards compatibility and sanity check
			increment = step;
		} else {
			increment = step*numCachedValues;
		}
		long currentValue;
		synchronized (READ_WRITE_LOCK) {
			try {
				openFile();
				
				// get
				buffer.clear();
				io.position(0);
				io.read(buffer);
				buffer.flip();
				currentValue = buffer.getLong();

				// set
				buffer.clear();
				buffer.putLong(currentValue + increment);
				buffer.flip();
				io.position(0);
				io.write(buffer);
			} finally {
				closeFile();
			}
		}
		return currentValue;
	}

	/**
	 * Never call from outside. Use {@link #createSynchronizer(SimpleSequence)} instead.
	 * This method sets the value of sequence if the persisted file exists.
	 * 
	 * @param seq
	 * @throws IOException
	 */
	private void init(SimpleSequence seq) throws IOException {
		synchronized (READ_WRITE_LOCK) {
			if (!javaFile.exists()) {
				logger.info("Sequence file " + seq.getFilename() + " doesn't exist. Creating new file.");
				javaFile.createNewFile();
				flushValue(seq.currentValueLong());
			} else {
				seq.sequenceValue = getCurrentValue();
			}
		}
	}

	/**
	 * Should only be used for initialization of sequence values.
	 * Use {@link #getAndSet(int)} for advancing sequences to higher values.
	 * @return
	 * @throws IOException
	 */
	private long getCurrentValue() throws IOException {
		synchronized (READ_WRITE_LOCK) {
			try {
				openFile();

				buffer.clear();
				io.position(0);
				io.read(buffer);
				buffer.flip();
				return buffer.getLong();
			} finally {
				closeFile();
			}
		}
	}

	/**
	 * Prepares a Synchronizer for the sequence.
	 * @param seq
	 * @return
	 * @throws IOException
	 */
	public static SimpleSequenceSynchronizer createSynchronizer(SimpleSequence seq) throws IOException {
		URL contextURL = (seq.getGraph() != null) ? seq.getGraph().getRuntimeContext().getContextURL() : null;
		String filename = seq.getFilename();
		String file = FileUtils.getFile(contextURL, filename);
		
		// extracting absolute path used as an ID
		File javaFile = new File(file);
		String pathIdentifier;
		try {
			pathIdentifier = javaFile.getCanonicalPath();
		} catch (Exception e) {
			logger.debug("Can't determine unique sequence file identifier: " + e.getMessage());
			pathIdentifier = javaFile.getAbsolutePath();
		}
		
		SimpleSequenceSynchronizer synchro = new SimpleSequenceSynchronizer(javaFile, pathIdentifier);
		synchro.init(seq);
		return synchro;
	}

	/**
	 * Should only be used for reset. Use {@link #getAndSet(int)} for persisting sequence values.
	 * @param value
	 * @throws IOException
	 */
	public final void flushValue(long value) throws IOException {
		synchronized (READ_WRITE_LOCK) {
			try {
				openFile();

				buffer.clear();
				buffer.putLong(value);
				buffer.flip();
				io.position(0);
				io.write(buffer);
			} finally {
				closeFile();
			}
		}
	}

	/**
	 * Unregisters sequence. Returns range if possible.
	 * 
	 * @param seq
	 */
	public final void freeSequence(SimpleSequence seq) {
		try {
			boolean rangeReturned = tryReturnRange(seq);
			if (rangeReturned) {
				logger.debug("Part of sequence range successfully returned by sequence " + seq.getId());
			}
		} catch (IOException e) {
			logger.debug("Couldn't return unused range for sequence " + seq.getId());
		}
	}
	
	/**
	 * Checks whether the last range was provided to the same sequence. If yes, persists unused part of the range back to the file.
	 * 
	 * @param seq
	 * @return
	 * @throws IOException
	 */
	private boolean tryReturnRange(SimpleSequence seq) throws IOException {
		synchronized (READ_WRITE_LOCK) {
			try {
				openFile();
				
				// get current value
				buffer.clear();
				io.position(0);
				io.read(buffer);
				buffer.flip();
				long persistedValue = buffer.getLong();
				long endOfRange = seq.getEndOfCurrentRange();
				
				if (persistedValue == endOfRange + seq.step && seq.counter != 0) {
					buffer.clear();
					buffer.putLong(seq.currentValueLong() + seq.step);
					buffer.flip();
					io.position(0);
					io.write(buffer);
					return true;
				}
				return false;

			} finally {
				closeFile();
			}
		}
	}
}
