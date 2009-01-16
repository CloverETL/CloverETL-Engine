package org.jetel.component;

import java.util.ArrayList;

import org.jetel.data.DataRecord;

/**
 * A fixes size pool of arrays (stripes) of DataRecord objects
 * 
 * Each stripe can be locked so noone can access them Unlocked stripe can be
 * obtained via getStripe(int) - if the stripe is locked the method blocks until
 * its unlocked with release(int)
 * 
 * Number of stripes cannot be changed... a stripe is an ArrayList
 * 
 * @author pnajvar
 * 
 */

public class DataRecordArrayPool {

	private Stripe[] stripes;

	boolean open = false;
	
	public DataRecordArrayPool(int numberOfStripes, int stripeCapacity) {
		this.initialCapacity = stripeCapacity;
		buildStripes(numberOfStripes, stripeCapacity);
	}

	int initialCapacity;
	
	/**
	 * Return number of stripes in this pool
	 * 
	 * @return Number of stripes in pool
	 */
	public int size() {
		return this.stripes.length;
	}

	public void open() {
		this.open = true;
	}
	
	public synchronized void close() {
		this.open = false;
		notifyAll();
	}
	
	public boolean isOpen() {
		return this.open;
	}
	

	/**
	 * Returns arbitrary stripe that is not locked and is not empty (Stripe.getSize() > 0)
	 * If all stripe are unlocked and empty returns
	 * 
	 * @return
	 */
	public synchronized Stripe getNextFullStripe() {
		notify();
		Stripe stripe;
		boolean atLeastOneLocked = false;
		try {
			do {
				atLeastOneLocked = false;
				for (int i = 0; i < this.stripes.length; i++) {
					if (! this.stripes[i].isLocked()) {
						if (! this.stripes[i].isEmpty()) {
							this.stripes[i].setLocked(true);
							return this.stripes[i];
						}
					} else {
						atLeastOneLocked = true;
					}
				}
				if (! atLeastOneLocked) {
					if (! isOpen()) {
						// if all stripes are empty and unlocked
						// and we are closed then we won't wait for a full
						return null;
					} else {
						// this is a dump way how to avoid deadlock
						//System.err.println("DataRecordArrayPool.getNextFullStripe(): I don't like what I see - we're still open but there is no full stripe or locked stripe, will wait some time and retry.");
						wait();
						if (! this.isOpen()) {
							return null;
						}
					}
				} else {
					// we will politely wait for someone to release a stripe and we'll see then
					wait();
					if (! this.isOpen()) {
						return null;
					}
				}
			} while (true);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null; // in case we get interrupted
	}

	/**
	 * Returns arbitrary stripe that is not locked and is empty (Stripe.getSize() == 0)
	 * 
	 * @return
	 */
	public synchronized Stripe getNextEmptyStripe() {
		notify();
		Stripe stripe;
		try {
			do {
				for (int i = 0; i < this.stripes.length; i++) {
					if (! this.stripes[i].isLocked() && this.stripes[i].isEmpty()) {
						this.stripes[i].setLocked(true);
						return this.stripes[i];
					}
				}
				wait();
				if (! this.isOpen()) {
					return null;
				}
			} while (true);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null; // in case we get interrupted
	}

	/**
	 * Release a stripe obtained by getStripe(int) call Each caller of
	 * getStripe(int) is obliged to release the stripe when they don't work with
	 * it
	 * 
	 * @param i
	 */
	public synchronized void releaseStripe(int i) {
		this.stripes[i].setLocked(false);
		notifyAll();
	}

	Stripe lockOrReturn(int i) {
		Stripe s = this.stripes[i];
		synchronized(s) {
			if (! s.isLocked()) {
				s.setLocked(true);
				return s;
			} else {
				return null;
			}
		}
	}

	/**
	 * Populate all stripes with data record duplicates
	 * 
	 * @param template
	 */
	public void populate(DataRecord template) {
		Stripe s;
		DataRecord[] a;
		for(int i = 0; i < this.stripes.length; i++) {
			s = this.stripes[i];
			a = s.getData();
			for(int j = 0; j < initialCapacity; j++) {
				a[j] = template.duplicate();
			}
			s.release();
		}
	}

	void buildStripes(int numberOfStripes, int stripeCapacity) {
		this.stripes = new Stripe[numberOfStripes];
		for (int i = 0; i < this.stripes.length; i++) {
			this.stripes[i] = new Stripe(stripeCapacity, i, this);
		}
	}

	/**
	 * This class describes stripe state and its (real) size
	 * 
	 * Stripe size might be hard to determine since the underlying arraylist can
	 * have fixed length but only part of it can be truly used - the size of the
	 * used part can be obtained (and MUST be set) via its descriptor
	 * 
	 * @author pnajvar
	 * 
	 */
	public static class Stripe {
		boolean locked = false;
		int size = 0;
		DataRecord[] data;
		int index;
		DataRecordArrayPool parent;
		
		Stripe(int capacity, int index, DataRecordArrayPool parent) {
			this.data = new DataRecord[capacity];
			this.index = index;
			this.parent = parent;
			this.locked = false;
			this.size = 0;
		}

		public void setSize(int size) {
			this.size = size;
		}

		public int getSize() {
			return this.size;
		}

		public boolean isLocked() {
			return locked;
		}
		
		public boolean isEmpty() {
			return this.size == 0;
		}

		public void clear() {
			setSize(0);
		}
		
		boolean setLocked(boolean locked) {
			boolean old = this.locked;
			this.locked = locked;
			return old;
		}

		public DataRecord[] getData() {
			return this.data;
		}
		
		public void release() {
			this.parent.releaseStripe(this.index);
		}

		public String toString() {
			return "Stripe[" + index + ",size=" + getSize() + ",capacity=" + data.length + "]";
		}
		
	}

}
