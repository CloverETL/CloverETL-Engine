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
package org.jetel.data;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jetel.test.CloverTestCase;
import org.jetel.util.bytes.CloverBuffer;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20.5.2013
 */
public class PersistentBufferQueueTest extends CloverTestCase {

	private static final int NUM_ITERATIONS = 10000;
	
	public void testSingleThread() {
		PersistentBufferQueue queue = new PersistentBufferQueue();
		CloverBuffer buffer = CloverBuffer.allocate(10);
		
		for (int j = 0; j < NUM_ITERATIONS; j++) {
			for (int i = 0; i < 11; i++) {
				buffer.clear();
				buffer.putInt(i * j);
				buffer.flip();
				assertTrue(queue.offer(buffer));
			}
			
			for (int i = 0; i < 11; i++) {
				buffer.clear();
				assertTrue(queue.poll(buffer) == buffer);
				assertTrue(buffer.limit() == 4);
				assertTrue(buffer.getInt() == i * j);
			}
			assertTrue(queue.poll(buffer) == null);
		}
		queue.close();
	}

	public void testMultiThread() throws InterruptedException, ExecutionException {
		final PersistentBufferQueue queue = new PersistentBufferQueue();
		
		Callable<Void> producent = new Callable<Void>() {
			@Override
			public Void call() {
				CloverBuffer buffer = CloverBuffer.allocate(10);
				for (int i = 0; i < NUM_ITERATIONS; i++) {
					buffer.clear();
					buffer.putInt(i);
					buffer.flip();
					assertTrue(queue.offer(buffer));
				}
				return null;
			}
		};
		Callable<Void> consument = new Callable<Void>() {
			@Override
			public Void call() {
				CloverBuffer buffer = CloverBuffer.allocate(10);
				for (int i = 0; i < NUM_ITERATIONS; i++) {
					buffer.clear();
					while(queue.poll(buffer) == null);
					assertTrue(buffer.limit() == 4);
					assertTrue(buffer.getInt() == i);
				}
				
				assertTrue(queue.poll(buffer) == null);
				return null;
			}
		};
		
		ExecutorService pool = Executors.newFixedThreadPool(2);
		pool.submit(producent).get();
		pool.submit(consument).get();
		queue.close();
	}

	public void testGrowingBuffers() throws InterruptedException, ExecutionException {
		final PersistentBufferQueue queue = new PersistentBufferQueue();
		
		Callable<Void> producent = new Callable<Void>() {
			@Override
			public Void call() {
				CloverBuffer buffer = CloverBuffer.allocate(10);
				int size = 1;
				boolean grow = true;
				for (int i = 0; i < NUM_ITERATIONS; i++) {
					if (size > 10000) {
						grow = false;
					}
					if (size < 1) {
						size = 1;
						grow = true;
					}
					if (grow) {
						size *= 2;
					} else {
						size /= 2;
					}
					buffer.clear();
					for (int j = 0; j < size; j++) {
						buffer.put((byte) (j % 127));
					}
					buffer.flip();
					assertTrue(queue.offer(buffer));
				}
				return null;
			}
		};
		Callable<Void> consument = new Callable<Void>() {
			@Override
			public Void call() {
				CloverBuffer buffer = CloverBuffer.allocate(10);
				int size = 1;
				boolean grow = true;
				for (int i = 0; i < NUM_ITERATIONS; i++) {
					if (size > 10000) {
						grow = false;
					}
					if (size < 1) {
						size = 1;
						grow = true;
					}
					if (grow) {
						size *= 2;
					} else {
						size /= 2;
					}
					buffer.clear();
					while(queue.poll(buffer) == null);
					assertTrue(buffer.limit() == size);
					for (int j = 0; j < size; j++) {
						assertTrue(buffer.get() == (j % 127));
					}
				}
				
				assertTrue(queue.poll(buffer) == null);
				return null;
			}
		};
		
		ExecutorService pool = Executors.newFixedThreadPool(2);
		pool.submit(producent).get();
		pool.submit(consument).get();
		queue.close();
	}

}
