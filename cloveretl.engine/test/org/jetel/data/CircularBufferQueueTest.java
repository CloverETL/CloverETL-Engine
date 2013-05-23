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

import org.jetel.test.CloverTestCase;
import org.jetel.util.bytes.CloverBuffer;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20.5.2013
 */
public class CircularBufferQueueTest extends CloverTestCase {

	public void testSingleThread() {
		CircularBufferQueue queue = new CircularBufferQueue(100);
		CloverBuffer buffer = CloverBuffer.allocate(10);
		
		for (int j = 0; j < 10000; j++) {
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
		}
	}

	public void testMultiThread() throws InterruptedException {
		final CircularBufferQueue queue = new CircularBufferQueue(100);
		
		Thread producent = new Thread(new Runnable() {
			@Override
			public void run() {
				CloverBuffer buffer = CloverBuffer.allocate(10);
				for (int i = 0; i< 10000; i++) {
					buffer.clear();
					buffer.putInt(i);
					buffer.flip();
					while (!queue.offer(buffer));
				}
			}
		});
		Thread consument = new Thread(new Runnable() {
			@Override
			public void run() {
				CloverBuffer buffer = CloverBuffer.allocate(10);
				for (int i = 0; i < 10000; i++) {
					buffer.clear();
					while(queue.poll(buffer) == null);
					assertTrue(buffer.limit() == 4);
					assertTrue(buffer.getInt() == i);
				}
			}
		});
		producent.start();
		consument.start();
		producent.join();
		consument.join();
	}

}
