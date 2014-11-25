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
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.util.file.FileUtils;

/**
 * Wrapper class for I/O of persisted sequences.
 * Allows safe sharing of the same sequence file by multiple threads.
 * 
 * @author salamonp (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 21. 11. 2014
 */
public class SimpleSequenceSynchronizer {
	// each file has its own synchronizer, use absolute pathnames as keys
	private static final HashMap<String, SimpleSequenceSynchronizer> synchronizerHolder = new HashMap<>();

	// every synchronizer has a set of registered sequences
	private static final HashMap<SimpleSequenceSynchronizer, Set<SimpleSequence>> sequenceHolder = new HashMap<>();

	private FileLock lock;
	private FileChannel io;
	private ByteBuffer buffer;
	private String file; // absolute pathname

	private final Object READ_WRITE_LOCK = new Object();
	private static final Log logger = LogFactory.getLog(SimpleSequenceSynchronizer.class);
    private static final int DATA_SIZE = 8; //how many bytes occupy serialized value in file
    private static final String ACCESS_MODE="rwd";

	private SimpleSequenceSynchronizer(String file) {
		this.file = file;
		buffer = ByteBuffer.allocateDirect(DATA_SIZE);
	}

	/**
	 * Ensures the FileChannel used for I/O is open. The channel gets automatically closed when
	 * thread blocked by I/O operation receives an interruption. E.g. when user
	 * aborts a running graph, we need to re-open the FileChannel when another
	 * graph is still using this synchronizer.
	 * @throws IOException
	 */
	private void ensureOpen() throws IOException {
		int limit = 10;
		synchronized (READ_WRITE_LOCK) {
			while (!io.isOpen() && limit > 0) {
				free();
				io = new RandomAccessFile(file, ACCESS_MODE).getChannel();
				lock = io.tryLock();
				if (lock == null) {
					logger.warn("Can't obtain file lock for sequence file " + file);
				}
			}
		}
		if (limit == 0) {
			logger.warn("Can't open sequence file " + file);
		}
	}

	/**
	 * "Atomic" get and set operation. Stores incremented value and returns start of new sequence range.
	 * Use this method to advance sequence forward.
	 * @param increment
	 * @return
	 * @throws IOException
	 */
	public long getAndSet(int step, int numCachedValues) throws IOException {
		long increment;
		if (numCachedValues <= 0) {
			// backwards compatibility and sanity check
			increment = step;
		} else {
			increment = step*numCachedValues;
		}
		long currentValue;
		synchronized (READ_WRITE_LOCK) {
			ensureOpen();

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
		}
		return currentValue;
	}

	/**
	 * Never call from outside. Use {@link #registerAndGetSynchronizer(SimpleSequence)} instead.
	 * 
	 * @param seq
	 * @throws IOException
	 */
	private void init(SimpleSequence seq) throws IOException {
		try {
			File javaFile = new File(file);
			if (!javaFile.exists()) {
				logger.info("Sequence file " + seq.getName() + " doesn't exist. Creating new file.");
				javaFile.createNewFile();
				io = new RandomAccessFile(file, ACCESS_MODE).getChannel();
				lock = io.lock();
				io.force(true);
				flushValue(seq.currentValueLong());
			} else {
				io = new RandomAccessFile(file, ACCESS_MODE).getChannel();
				lock = io.tryLock();
				if (lock == null) {
					// report non-locked sequence
					logger.warn("Can't obtain file lock for sequence: " + seq.getName() + " id: " + seq.getId());
				}
			}
		} catch (IOException | BufferUnderflowException e) {
			free();
			throw e;
		}
	}

	/**
	 * Should only be used for initialization of sequence values.
	 * Use {@link #getAndSet(int)} for advancing sequences to higher values.
	 * @return
	 * @throws IOException
	 */
	public long getCurrentValue() throws IOException {
		synchronized (READ_WRITE_LOCK) {
			ensureOpen();
			
			io.force(true);
			buffer.clear();
			io.position(0);
			io.read(buffer);
			buffer.flip();
			return buffer.getLong();
		}
	}

	/**
	 * Registers sequence to use a Synchronizer.
	 * @param seq
	 * @return
	 * @throws IOException
	 */
	public static SimpleSequenceSynchronizer registerAndGetSynchronizer(SimpleSequence seq) throws IOException {
		URL contextURL = (seq.getGraph() != null) ? seq.getGraph().getRuntimeContext().getContextURL() : null;
		String filename = seq.getFilename();
		String file = FileUtils.getFile(contextURL, filename);
		SimpleSequenceSynchronizer synchro;

		synchronized (synchronizerHolder) {
			// get or create synchronizer
			synchro = synchronizerHolder.get(file);
			if (synchro == null) {
				synchro = new SimpleSequenceSynchronizer(file);
				synchro.init(seq);
				synchronizerHolder.put(file, synchro);
			}
		}
		synchronized (sequenceHolder) {
			// register this sequence
			Set<SimpleSequence> registeredSequences = sequenceHolder.get(synchro);
			if (registeredSequences == null) {
				registeredSequences = new HashSet<>();
				sequenceHolder.put(synchro, registeredSequences);
			}
			registeredSequences.add(seq);
		}
		return synchro;
	}

	/**
	 * Should only be used for reset. Use {@link #getAndSet(int)} for persisting sequence values.
	 * @param value
	 * @throws IOException
	 */
	public final void flushValue(long value) throws IOException {
		synchronized (READ_WRITE_LOCK) {
			ensureOpen();
			
			buffer.clear();
			buffer.putLong(value);
			buffer.flip();
			io.position(0);
			io.write(buffer);
		}
	}

	/**
	 * Unregisters sequence. Frees held resources if the passed sequence was the only user of this Synchronizer.
	 * 
	 * @param seq
	 */
	public final void unregisterSequence(SimpleSequence seq) {
		boolean lastSequence = false;

		synchronized (sequenceHolder) {
			Set<SimpleSequence> registeredSequences = sequenceHolder.get(this);
			if (registeredSequences == null) {
				// something went wrong, this should never happen
				logger.debug("Registered sequences for SimpleSequence synchronizer not found.");
				return;
			}
			registeredSequences.remove(seq);
			if (registeredSequences.size() == 0) {
				lastSequence = true;
				try {
					free();
				} catch (IOException ex) {
					logger.warn("I/O error when freeing sequence " + seq.getName(), ex);
				}
				sequenceHolder.remove(this);
			}
		}

		if (lastSequence) {
			synchronized (synchronizerHolder) {
				synchronizerHolder.remove(file);
			}
		}
	}

	/**
	 * Never call from outside. Use {@link #unregisterSequence(SimpleSequence)} instead.
	 * @return
	 * @throws IOException
	 */
	private void free() throws IOException {
		synchronized (READ_WRITE_LOCK) {
			if (lock != null && lock.isValid()) {
				lock.release();
				lock = null;
			}
			if (io != null && io.isOpen()) {
				io.close();
				io = null;
			}
		}
	}
}
