/*
 * CloverETL Engine - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com).  Use is subject to license terms.
 *
 * www.cloveretl.com
 */
package org.jetel.collection;

import java.util.EmptyStackException;
import java.util.LinkedList;
import java.util.NoSuchElementException;

/**
 * Stack implementation using unsynchronized {@link LinkedList}.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 13.3.2013
 */
public class Stack<T> {

	protected LinkedList<T> list = new LinkedList<T>();
	
	public T pop() throws EmptyStackException {
		try {
			return list.removeLast();
		} catch (NoSuchElementException e) {
			throw new EmptyStackException();
		}
	}
	
	public void push(T value) {
		list.add(value);
	}
	
	public T peek() throws EmptyStackException {
		try {
			return list.getLast();
		} catch (NoSuchElementException e) {
			throw new EmptyStackException();
		}
	}
	
	public int size() {
		return list.size();
	}
	
	public boolean isEmpty() {
		return list.isEmpty();
	}
	
	public void clear() {
		list.clear();
	}
	
	@Override
	public String toString() {
		return list.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((list == null) ? 0 : list.hashCode());
		return result;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Stack other = (Stack) obj;
		if (list == null) {
			if (other.list != null)
				return false;
		} else if (!list.equals(other.list))
			return false;
		return true;
	}
}
