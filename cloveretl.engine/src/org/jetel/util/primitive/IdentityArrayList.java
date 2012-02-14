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

import java.util.ArrayList;
import java.util.Collection;

/**
 * Custom array list implementation which identifies elements by identity (==)
 * instead of equal() method.
 * 
 * @author Kokon (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @param <E>
 * @created 30 Jan 2012
 */
public class IdentityArrayList<E> extends ArrayList<E> {

	private static final long serialVersionUID = -1306337203859998463L;

	public IdentityArrayList() {
		super();
	}

    public IdentityArrayList(Collection<? extends E> c) {
		super(c);
	}

    public IdentityArrayList(int initialCapacity) {
		super(initialCapacity);
	}

	@Override
	public boolean remove(Object o) {
		return super.remove(this.indexOf(o)) != null;
	}

	@Override
	public boolean contains(Object o) {
		return indexOf(o) >= 0;
	}

	@Override
	public int indexOf(Object o) {
		for (int i = 0; i < size(); i++) {
			if (o == get(i)) {
				return i;
			}
		}
		return -1;
	}

	@Override
	public int lastIndexOf(Object o) {
		for (int i = size() - 1; i >= 0; i--) {
			if (o == get(i)) {
				return i;
			}
		}
		return -1;
	}

}
