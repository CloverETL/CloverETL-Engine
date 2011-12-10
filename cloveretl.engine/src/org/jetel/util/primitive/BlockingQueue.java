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
package org.jetel.util.primitive;

/**
 * Implementation of simple queue (First In First Out - FIFO), bounded and blocking queue.
 * 
 * @author Martin Zatopek (Javlin Consulting s.r.o)
 * @since  December 5, 2005
 */
public class BlockingQueue extends Queue {

	
	/**
	 * Constructor.
	 * @param size maximum length of the FIFO
	 */
	public BlockingQueue(int size) {
		super(size);
	}

	/**
	 * Adds item at the end of FIFO.
	 * If queue is full wait.
	 * @see org.jetel.util.primitive.Queue#add(java.lang.Object)
	 */
	@Override
	synchronized public boolean add(Object item) {
		while(isFull()) {
			try {
				wait();
			} catch (InterruptedException e) {
				return false;
			}
		}
		notifyAll();
		return super.add(item);
	}

	/**
	 * Gets the next element stored in the FIFO (in FIFO order) - the oldest one
	 * If queue is empty wait.
	 * @return Oldest object in the FIFO, null upon empty FIFO
	 * @see org.jetel.util.primitive.Queue#get()
	 */
	@Override
	synchronized public Object get() {
		while(isEmpty()) {
			try {
				wait();
			} catch (InterruptedException e) {
				return null;
			}
		}
		notifyAll();
		return super.get();
	}
	
	/**
	 * Gets the next element stored in the FIFO (in FIFO order) - the oldest one
	 * @return Oldest object in the FIFO, null upon empty FIFO
	 */
	synchronized public Object get(long timeout) {
		if(isEmpty()) {
			try {
				wait(timeout);
				if(isEmpty()) return null;
			} catch (InterruptedException e) {
				return null;
			}
		}
		notifyAll();
		return super.get();
	}

	/**
	 * @see org.jetel.util.primitive.Queue#isEmpty()
	 */
	@Override
	synchronized public boolean isEmpty() {
		return super.isEmpty();
	}
	
	/**
	 * @see org.jetel.util.primitive.Queue#isFull()
	 */
	@Override
	synchronized public boolean isFull() {
		return super.isFull();
	}
	
	/**
	 * @see org.jetel.util.primitive.Queue#removeAll()
	 */
	@Override
	synchronized public void removeAll() {
		super.removeAll();
	}
	
	/**
	 * Only test used.
	 * @param args
	 */
	public static void main(String[] args) {
		BlockingQueue queue = new BlockingQueue(10);
		
		Thread p1 = new Producent(queue, 1);
		Thread p2 = new Producent(queue, 2);
		Thread p3 = new Producent(queue, 3);
		
		Thread c = new Consument(queue);

		c.start();
		p1.start();
		p2.start();
		p3.start();
	}
}

class Producent extends Thread {
	private int idx;
	private BlockingQueue queue;
	
	public Producent(BlockingQueue queue, int idx) {
		super();
		this.idx = idx;
		this.queue = queue;
	}

	@Override
	public void run() {
		int i = 0;
		for(;;) {
			
			StringBuffer s = new StringBuffer();
			for(int j = 0; j < idx; j++) {
				s.append("\t");
			}
			s.append(idx + " " + i);
			queue.add(s);
			i++;
		}
	}
}

class Consument extends Thread {
	private BlockingQueue queue;

	public Consument(BlockingQueue queue) {
		super();
		this.queue = queue;
	}

	@Override
	public void run() {
		for(;;) {
			System.out.println(queue.get());
			try {
				sleep(200);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
