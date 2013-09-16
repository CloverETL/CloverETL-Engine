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
/*******************************************************************************
 * Copyright 2010 Cees De Groot, Alex Boisvert, Jan Kotek
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.jetel.component.tree.writer.portdata;

import java.io.IOException;
import java.util.Iterator;

import jdbm.RecordManager;
import jdbm.Serializer;
import jdbm.helper.LongHashMap;
import jdbm.helper.RecordManagerImpl;
import jdbm.recman.BaseRecordManager;

import org.jetel.component.tree.writer.util.JdbmCloser;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.util.MemoryUtils;

/**
 * A RecordManager wrapping and caching another RecordManager.
 * 
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 19 Sep 2011
 */
public class CacheRecordManager extends RecordManagerImpl {
	
	private static final int INITIAL_CACHE_SIZE = 1024;

	/** Wrapped RecordManager */
	protected RecordManager recman;

	/** Cached object hashtable */
	protected LongHashMap<CacheEntry> hash;

	/** Beginning of linked-list of cache elements. First entry is element which has been used least recently.*/
	protected CacheEntry first;
	/** End of linked-list of cache elements. Last entry is element which has been used most recently. */
	protected CacheEntry last;
	
	private long writeCache;
	private long availableMemory;

	/**
	 * Construct a CacheRecordManager wrapping another RecordManager and using a given cache policy.
	 * 
	 * @param recman
	 *            Wrapped RecordManager
	 * @param cache
	 *            Cache policy
	 */
	CacheRecordManager(RecordManager recman, long cacheSize) {
		if (recman == null) {
			throw new IllegalArgumentException("Argument 'recman' is null");
		}
		this.hash = new LongHashMap<CacheEntry>(INITIAL_CACHE_SIZE);
		this.recman = recman;
		this.availableMemory = cacheSize;
	}
	
	public static CacheRecordManager createInstance(String tempFile, long cacheSize) throws IOException {
		BaseRecordManager recMan = new BaseRecordManager(tempFile);
		recMan.disableTransactions();
		return new CacheRecordManager(recMan, cacheSize);
	}
	
	private void moveToWriteCache(Object obj, boolean isDirty) {
		if (obj instanceof BPage<?, ?>) {
			BPage<?, ?> page = (BPage<?, ?>) obj;
			changeReadCache(-(page.lastMemoryUsage + MemoryUtils.CACHE_ENTRY_OVERHEAD));
			if (isDirty) {
				changeWriteCache(page.lastMemoryUsage);
			}
		} else {
			changeReadCache(-MemoryUtils.CACHE_ENTRY_OVERHEAD); // FIX of CL-2404: obj needs not to be BPage only (can be BTree)
		}
	}

	private void changeReadCache(long delta) {
		availableMemory -= delta;
	}

	private void changeWriteCache(long delta) {
		writeCache += delta;
		availableMemory -= delta;
	}

	private boolean reuse(long delta) {
		if (availableMemory < delta) {
			return true;
		}
		return false;
	}
	
	private void evictIfNeeded() throws IOException {
		while (availableMemory < 0) {
			if (first != null) {
				purgeEntry();
			} else if (writeCache == 0) {
				// Should not happen: no more memory available, but cache is empty. At least prevent infinite loop as in issue CL-2404
				throw new JetelRuntimeException("Cache management error (CL-2404)");
			}
			if (writeCache > 0) {
				recman.commit();
				availableMemory += writeCache;
				writeCache = 0;
			}
		}
	}

	/**
	 * Get the underlying Record Manager.
	 * 
	 * @return underlying RecordManager or null if CacheRecordManager has been closed.
	 */
	public RecordManager getRecordManager() {
		return recman;
	}

	@Override
	public synchronized <A> long insert(A obj, Serializer<A> serializer) throws IOException {
		checkIfClosed();
		long recid = recman.insert(obj, serializer);
		
		if (obj instanceof BPage<?, ?>) {
			changeWriteCache(((BPage<?, ?>) obj).actualMemoryUsage);
		}
		evictIfNeeded();
		// DONT use cache for inserts, it usually hurts performance on batch inserts
		
		return recid;
	}

	@Override
	public synchronized <A> A fetch(long recid, Serializer<A> serializer, boolean disableCache) throws IOException {
		if (disableCache) {
			return recman.fetch(recid, serializer, disableCache);
		} else {
			return fetch(recid, serializer);
		}
	}

	@Override
	public synchronized void delete(long recid) throws IOException {
		throw new UnsupportedOperationException("Not implemented!"); 
		// Add memory handling when implementing this method
//		checkIfClosed();
//
//		_recman.delete(recid);
//		CacheEntry entry = _hash.get(recid);
//		if (entry != null) {
//			removeEntry(entry);
//			_hash.remove(entry._recid);
//		}
	}

	@Override
	public synchronized <A> void update(long recid, A obj, Serializer<A> serializer) throws IOException {
		checkIfClosed();
		CacheEntry entry = cacheGet(recid);
		if (entry != null) {
			// reuse existing cache entry
			long oldUsage = 0;
			long newUsage = 0;
			if (entry._obj instanceof BPage<?, ?>) {
				oldUsage = ((BPage<?, ?>) entry._obj).lastMemoryUsage;
			}
			if (obj instanceof BPage<?, ?>) {
				newUsage = ((BPage<?, ?>) obj).actualMemoryUsage;
				((BPage<?, ?>) obj).lastMemoryUsage = newUsage;
			}
			
			changeReadCache(newUsage - oldUsage);
			entry._obj = obj;
			entry._serializer = serializer;
			entry._isDirty = true;
		} else {
			cachePut(recid, obj, serializer, true);
		}
		evictIfNeeded();
	}

	@Override
	@SuppressWarnings("unchecked")
	public synchronized <A> A fetch(long recid, Serializer<A> serializer) throws IOException {
		checkIfClosed();

		CacheEntry entry = (CacheEntry) cacheGet(recid);
		if (entry == null) {
			A value = recman.fetch(recid, serializer);
			cachePut(recid, value, serializer, false);
			evictIfNeeded();
			return value;
		} else {
			return (A) entry._obj;
		}
	}

	@Override
	public synchronized void close() throws IOException {
		checkIfClosed();

		//recman.close();
		JdbmCloser.fastCloseUsingReflection((BaseRecordManager)recman);
		recman = null;
		hash = null;
	}

	@Override
	public synchronized void commit() throws IOException {
		checkIfClosed();
		updateCacheEntries();
		recman.commit();
		
		availableMemory += writeCache;
		writeCache = 0;
	}

	@Override
	public synchronized void rollback() throws IOException {
		checkIfClosed();

		recman.rollback();

		// discard all cache entries since we don't know which entries
		// where part of the transaction
		hash.clear();
		first = null;
		last = null;
	}

	@Override
	public synchronized long getNamedObject(String name) throws IOException {
		checkIfClosed();

		return recman.getNamedObject(name);
	}

	@Override
	public synchronized void setNamedObject(String name, long recid) throws IOException {
		checkIfClosed();

		recman.setNamedObject(name, recid);
	}

	/**
	 * Check if RecordManager has been closed. If so, throw an IllegalStateException
	 */
	private void checkIfClosed() throws IllegalStateException {
		if (recman == null) {
			throw new IllegalStateException("RecordManager has been closed");
		}
	}

	/**
	 * Update all dirty cache objects to the underlying RecordManager.
	 */
	@SuppressWarnings("unchecked")
	protected void updateCacheEntries() throws IOException {
		Iterator<CacheEntry> iter = hash.valuesIterator();
		while (iter.hasNext()) {
			CacheEntry entry = iter.next();
			if (entry._isDirty) {
				recman.update(entry._recid, entry._obj, entry._serializer);
				entry._isDirty = false;
			}
		}
	}

	/**
	 * Obtain an object in the cache
	 */
	protected CacheEntry cacheGet(long key) {
		CacheEntry entry = hash.get(key);
		if (entry != null) {
			touchEntry(entry);
		}
		return entry;
	}

	/**
	 * Place an object in the cache.
	 * 
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected void cachePut(long recid, Object value, Serializer serializer, boolean dirty) throws IOException {
		CacheEntry entry = hash.get(recid);
		long oldSize = 0;
		long newSize = 0;
		
		if (value instanceof BPage) {
			newSize = ((BPage<?, ?>) value).actualMemoryUsage;
			((BPage<?, ?>) value).lastMemoryUsage = newSize;
		}
		
		if (entry != null) {
			if (entry._obj instanceof BPage<?, ?>) {
				oldSize = ((BPage<?, ?>) first._obj).lastMemoryUsage;
			}
			
			entry._obj = value;
			entry._serializer = serializer;
			touchEntry(entry);
			
			changeReadCache(newSize - oldSize);
		} else {
			if (first != null && first._obj instanceof BPage<?, ?>) {
				oldSize = ((BPage<?, ?>) first._obj).lastMemoryUsage;
			}
			
			if (reuse(newSize - oldSize)) {
				// purge and recycle entry
				entry = purgeEntry();
				entry._recid = recid;
				entry._obj = value;
				entry._isDirty = dirty;
				entry._serializer = serializer;
				changeReadCache(newSize + MemoryUtils.CACHE_ENTRY_OVERHEAD);
			} else {
				entry = new CacheEntry(recid, value, serializer, dirty);
				changeReadCache(newSize + MemoryUtils.CACHE_ENTRY_OVERHEAD);
			}
			addEntry(entry);
			hash.put(entry._recid, entry);
		}
	}

	/**
	 * Add a CacheEntry. Entry goes at the end of the list.
	 */
	protected void addEntry(CacheEntry entry) {
		if (first == null) {
			first = entry;
			last = entry;
		} else {
			last._next = entry;
			entry._previous = last;
			last = entry;
		}
	}

	/**
	 * Remove a CacheEntry from linked list
	 */
	protected void removeEntry(CacheEntry entry) {
		if (entry == first) {
			first = entry._next;
		}
		if (last == entry) {
			last = entry._previous;
		}
		CacheEntry previous = entry._previous;
		CacheEntry next = entry._next;
		if (previous != null) {
			previous._next = next;
		}
		if (next != null) {
			next._previous = previous;
		}
		entry._previous = null;
		entry._next = null;
	}

	/**
	 * Place entry at the end of linked list -- Most Recently Used
	 */
	protected void touchEntry(CacheEntry entry) {
		if (last == entry) {
			return;
		}
		removeEntry(entry);
		addEntry(entry);
	}

	/**
	 * Purge least recently used object from the cache
	 * 
	 * @return recyclable CacheEntry
	 */
	@SuppressWarnings("unchecked")
	protected CacheEntry purgeEntry() throws IOException {
		CacheEntry entry = first;
		if (entry == null) {
			// changeReadCache(MemoryUtils.CACHE_ENTRY_OVERHEAD); FIX of CL-2404: this is must be done in place where
				// the new CacheEntry is inserted into cache (in the cachePut() method, few lines below call to this method)
			return new CacheEntry(-1, null, null, false);
		}

		if (entry._isDirty) {
			recman.update(entry._recid, entry._obj, entry._serializer);
		}
		moveToWriteCache(entry._obj, entry._isDirty);
		removeEntry(entry);
		hash.remove(entry._recid);

		entry._obj = null;
		entry._serializer = null;
		entry._isDirty = false;
		return entry;
	}

	@SuppressWarnings("rawtypes")
	protected static final class CacheEntry {

		protected long _recid;
		protected Object _obj;

		protected Serializer _serializer;
		protected boolean _isDirty;

		protected CacheEntry _previous;
		protected CacheEntry _next;

		CacheEntry(long recid, Object obj, Serializer serializer, boolean isDirty) {
			_recid = recid;
			_obj = obj;
			_serializer = serializer;
			_isDirty = isDirty;
		}

	}

	@Override
	public synchronized void clearCache() throws IOException {
		// discard all cache entries since we don't know which entries
		// where part of the transaction
		while (hash.size() > 0) {
			purgeEntry();
		}

		first = null;
		last = null;

	}

	@Override
	public void defrag() throws IOException {
		commit();
		recman.defrag();
	}

}
