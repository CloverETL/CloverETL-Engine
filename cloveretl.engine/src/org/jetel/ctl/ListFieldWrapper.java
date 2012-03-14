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

import java.util.AbstractList;
import java.util.Collection;
import java.util.List;
import java.util.RandomAccess;

import org.jetel.data.ListDataField;
import org.jetel.data.primitive.Decimal;

/**
 * The wrapper converts CharSequences into Strings.
 * 
 * It is used to enable passing lists in input/output records
 * by reference.
 * 
 * It should hide any differences between
 * {@link ListDataField}'s ListDataFieldView
 * and a standard implementation of a List interface.
 * 
 * Note that this could mean different handling of equals()
 * on DataFields.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 23.1.2012
 */
public class ListFieldWrapper<T> extends AbstractList<T> implements RandomAccess {
	
	private List<T> parent;
	
	/**
	 * @param parent
	 */
	@SuppressWarnings("unchecked")
	public ListFieldWrapper(Object parent) {
		if (parent == null) {
			throw new NullPointerException("parent");
		}
		this.parent = (List<T>) parent;
	}
	
	@SuppressWarnings("unchecked")
	private static <T> T toCTL(T o) {
		if (o instanceof CharSequence) {
			return (T) o.toString(); // convert CharSequences to Strings
		} else if (o instanceof Decimal) {
			return (T) ((Decimal) o).getBigDecimalOutput(); // convert to BigDecimal
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
	public boolean add(T e) {
		return parent.add(e); // no need to convert
	}

	@Override
	public boolean remove(Object o) {
		int index = indexOf(o);
		if (index >= 0 && index < size()) {
			parent.remove(index);
			return true;
		}
		return false;
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		return parent.addAll(c); // no need to convert
	}

	@Override
	public boolean addAll(int index, Collection<? extends T> c) {
		return parent.addAll(index, c); // no need to convert
	}

	@Override
	public void clear() {
		parent.clear();
	}

	@Override
	public T remove(int index) {
		return toCTL(parent.remove(index));
	}

}
