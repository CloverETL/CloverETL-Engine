/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002,03  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.util;

/**
 *  Implementation of simple (standard) First In First Out (FIFO) queue
 *
 *@author     dpavlis
 *@created    23. july 2003
 */
public class Fifo {

	Object[] fifoArray;
	int fifoLength;
	int size;


	/**
	 *  Constructor for the Fifo object
	 *
	 *@param  size  Maximum length of the FIFO
	 */
	public Fifo(int size) {
		this.size = size;
		fifoArray = new Object[size];
		fifoLength = 0;
	}


	/**
	 *  Adds item at the beginning of FIFO
	 *
	 *@param  item  Description of the Parameter
	 */
	public void add(Object item) {
		if (fifoLength < size) {
			copyArray(fifoArray, 0, 1, fifoLength);
			fifoArray[0] = item;
			fifoLength++;
		} else {
			throw new RuntimeException("Fifo is already full !");
		}
	}


	/**
	 *  Gets the empty attribute of the Fifo object
	 *
	 *@return    The empty value
	 */
	public boolean isEmpty() {
		return fifoLength == 0 ? true : false;
	}


	/**
	 *  gets the next element stored in the FIFO (in FIFO order) - the oldest one
	 *
	 *@return    Description of the Return Value
	 */
	public Object get() {
		if (fifoLength > 0) {
			return fifoArray[--fifoLength];
		} else {
			return null;
		}
	}


	/**  removes all elements from the FIFO */
	public void removeAll() {
		fifoLength = 0;
	}


	/**
	 *  performs array copy (within the same array). Regions can partially overlap.
	 *
	 *@param  array   Description of the Parameter
	 *@param  from    Description of the Parameter
	 *@param  to      Description of the Parameter
	 *@param  length  Description of the Parameter
	 */
	private final void copyArray(Object[] array, int from, int to, int length) {
		
		System.arraycopy(array,from,array,to,length);
		//for (int i = length - 1; i >= 0; i--) {
		//	array[to + i] = array[from + i];
		//}
	}


	/**  Prints the content of the FIFO to stdout */
	public void dump() {
		for (int i = 0; i < fifoLength; i++) {
			System.out.println(fifoArray[i]);
		}
	}

	/*
	 *  public static void main(String[] args){
	 *  Fifo fifo=new Fifo(10);
	 *  fifo.add("Ahoj");
	 *  fifo.add("Nazdar");
	 *  fifo.add("Cau");
	 *  fifo.dump();
	 *  System.out.println("Get: "+fifo.get());
	 *  System.out.println("Get: "+fifo.get());
	 *  fifo.add("TePic");
	 *  fifo.dump();
	 *  System.out.println("Get: "+fifo.get());
	 *  fifo.dump();
	 *  }
	 */
}

