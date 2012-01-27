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
package org.jetel.ctl;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.jetel.util.string.CloverString;

/**
 * The wrapper converts CharSequences into Strings.
 * 
 * It is used to enable passing lists in input/output records
 * by reference.
 * 
 * FIXME removing elements by value will not work as expected.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 23.1.2012
 */
public class ListFieldWrapper<T> implements List<T> {
	
	private List<T> parent;
	
	/**
	 * @param parent
	 */
	@SuppressWarnings("unchecked")
	public ListFieldWrapper(List<Object> parent) {
		if (parent == null) {
			throw new NullPointerException("parent");
		}
		this.parent = (List<T>) parent;
	}
	
	@SuppressWarnings("unchecked")
	private static <T> T toCTL(T o) {
		if (o instanceof CharSequence) {
			return (T) o.toString(); // convert CharSequences to Strings
		}
		return o;
	}

	@SuppressWarnings("unchecked")
	private static <T> T fromCTL(T o) {
		if (o instanceof String) {
			return (T) new CloverString((String) o); // convert CharSequences to Strings
		}
		return o;
	}

	@Override
	public T get(int index) {
		return toCTL(parent.get(index));
	}

	@Override
	public int size() {
		return parent.size();
	}

	// only convert to CTL - underlying DataFields will convert Strings to CloverStrings 
	@Override
	public T set(int index, T element) {
		return toCTL(parent.set(index, element));
	}

	@Override
	public void add(int index, T element) {
		parent.add(index, element); // no need to convert
	}

	@Override
	public boolean isEmpty() {
		return parent.isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return parent.contains(fromCTL(o));
	}

	@Override
	public Iterator<T> iterator() {
		return listIterator();
	}

	@Override
	public Object[] toArray() {
		return parent.toArray();
	}

	@Override
	public <S> S[] toArray(S[] a) {
		return parent.toArray(a);
	}

	@Override
	public boolean add(T e) {
		return parent.add(e); // no need to convert
	}

	@Override
	public boolean remove(Object o) {
		return parent.remove(fromCTL(o));
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return parent.containsAll(c); // FIXME convert from CTL
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		return parent.addAll(c); // FIXME convert from CTL
	}

	@Override
	public boolean addAll(int index, Collection<? extends T> c) {
		return parent.addAll(index, c); // FIXME convert from CTL
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		return parent.removeAll(c); // FIXME convert from CTL
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		return parent.retainAll(c); // FIXME convert from CTL
	}

	@Override
	public void clear() {
		parent.clear();
	}

	@Override
	public T remove(int index) {
		return toCTL(parent.remove(index));
	}

	@Override
	public int indexOf(Object o) {
		return parent.indexOf(fromCTL(o));
	}

	@Override
	public int lastIndexOf(Object o) {
		return parent.lastIndexOf(fromCTL(o));
	}

	@Override
	public ListIterator<T> listIterator() {
		return new WrapperIterator<T>(parent.listIterator());
	}

	@Override
	public ListIterator<T> listIterator(int index) {
		return new WrapperIterator<T>(parent.listIterator(index));
	}

	@Override
	public List<T> subList(int fromIndex, int toIndex) {
		return parent.subList(fromIndex, toIndex);
	}


	private class WrapperIterator<S> implements ListIterator<S> {
		
		private ListIterator<S> parentIterator;
		
		private WrapperIterator(ListIterator<S> parentIterator) {
			this.parentIterator = parentIterator;
		}
		
		@Override
		public boolean hasNext() {
			return parentIterator.hasNext();
		}
		
		@Override
		public S next() {
			return toCTL(parentIterator.next());
		}
		
		@Override
		public boolean hasPrevious() {
			return parentIterator.hasPrevious();
		}
		
		@Override
		public S previous() {
			return toCTL(parentIterator.previous());
		}
		
		@Override
		public int nextIndex() {
			return parentIterator.nextIndex();
		}
		
		@Override
		public int previousIndex() {
			return parentIterator.previousIndex();
		}
		
		@Override
		public void remove() {
			parentIterator.remove();
		}
		
		@Override
		public void set(S e) {
			parentIterator.set(e); // no need to convert
		}
		
		@Override
		public void add(S e) {
			parentIterator.add(e); // no need to convert
		}
		
	}
	
}
