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
 *  Implementation of simple queue (First In First Out - FIFO).
 *
 *  @author Martin Zatopek (Javlin Consulting s.r.o)
 *  @since  December 5, 2005
 */
public class Queue {
	private final int size;
	private int length;
	private int front;
	private int rear;
	private final Object[] queue;
	
	/**
	 *  Constructor.
	 *
	 *  @param size maximum length of the queue
	 */
	public Queue(int size) {
		this.size = size;
		queue = new Object[size];
		length = 0;
		front = 0;
		rear = 0;
	}

	/**
	 * Cycle increment pointer into queue.
	 * @param i
	 * @return
	 */
	private int inc(int i) {
		return (++i == size) ? 0 : i;
	}
	
	/**
	 *  Adds item at the end of queue
	 *
	 *  @param  item  Object to add
	 */
	public boolean add(Object item) {
		if(item == null) {
			throw new IllegalArgumentException("Can't add null into queue.");
		}
		if(isFull()) return false;
		queue[front] = item;
		front = inc(front);
		length++;
		return true;
	}

	/**
	 *  Check whether the queue is empty.
	 *
	 *  @return whether queue is empty
	 */
	public boolean isEmpty() {
		return length == 0;
	}

	/**
	 *  Check whether the queue is full.
	 *
	 *  @return whether queue is full
	 */
	public boolean isFull() {
		return length == size;
	}

	/**
	 *  Gets the next element stored in the queue - the oldest one
	 *
	 *  @return oldest object in the queue, null upon empty FIFO
	 */
	public Object get() {
		if(isEmpty()) return null;
		Object ret = queue[rear];
		queue[rear] = null;
		rear = inc(rear);
		length--;
		return ret;
	}

	/**
	 *  removes all elements from the FIFO
	 */
	public void removeAll() {
		for(int i = 0; i < length; i++) {
			queue[rear] = null;
			rear = inc(rear);
		}
		length = 0;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		int r = rear;
		for(int i = 0; i < length; i++) {
			sb.append(queue[r]);
			sb.append("\n");
			r = inc(r);
		}
		return sb.toString();
	}
	
	/**
	 * Only test used.
	 * @param args
	 */
	public static void main(String[] args) {
		Queue q = new Queue(10);
		
		for(int i = 0; i < 12; i++) {
			System.out.println(q.add(Integer.valueOf(i)));
		}
		
		for(int i = 0; i < 12; i++) {
			System.out.println(q.get());
		}
		
		System.out.println("dump:\n" + q.toString());
	}
}