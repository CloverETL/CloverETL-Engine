/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
*    Lesser General Public License for more details.
*
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.util;

/**
 *  Implementation of simple (standard) First In First Out (FIFO) queue.
 *
 *  @author Sven Boden - Rewrite of the original implementation.
 *  @since  Nov 12, 2004
 */
public class Fifo {
	private int size;
	private Object[] fifoArray;
	private int front;
	private int rear;

	private int fifoLength;

	/**
	 *  Constructor for the Fifo object
	 *
	 *  @param  size  Maximum length of the FIFO
	 */
	public Fifo(int size) {
		this.size = size;
		fifoArray = new Object[size];
		front = 0;
		rear = -1;
		fifoLength = 0;
	}

	/**
	 *  Adds item at the end of FIFO
	 *
	 *  @param  item  Object to add
	 */
	public void add(Object item) {
		if ( fifoLength == size ) {
			throw new RuntimeException("Fifo is already full");
		}
		if (rear == size - 1) {
			rear = -1;
		}
		fifoArray[++rear] = item;
		fifoLength++;
	}

	/**
	 *  Check whether the FIFO is empty.
	 *
	 *  @return    whether FIFO is empty
	 */
	public boolean isEmpty() {
		return (fifoLength == 0);
	}

	/**
	 *  gets the next element stored in the FIFO (in FIFO order) - the oldest one
	 *
	 *  @return Oldest object in the FIFO, null upon empty FIFO
	 */
	public Object get() {
		if (fifoLength != 0) {
			Object temp = fifoArray[front++];
			if (front == size)
				front = 0;
			fifoLength--;
			return temp;
		}
		else {
			return null;
		}
	}

	/**
	 *  removes all elements from the FIFO
	 */
	public void removeAll() {
		fifoLength = 0;
		front = 0;
		rear = -1;
	}

	/**
	 *  Prints the content of the FIFO to stdout
	 */
	public void dump() {
		if (rear >= front) {
			// contiguous queue
			for (int i = front; i < fifoLength; i++) {
				System.out.println(fifoArray[i]);
			}
		}
		else  {
			// broken queue
			for (int i = front; i < size; i++) {
				System.out.println(fifoArray[i]);
			}
			for (int i = 0; i < rear + 1; i++) {
				System.out.println(fifoArray[i]);
			}
		}
	}
}