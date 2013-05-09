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
package org.jetel.component.validator.utils.comparators;

import java.util.Comparator;

/**
 * Comparator for longs. Uses singleton pattern.
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 15.1.2013
 */
public class LongComparator implements Comparator<Long>{
	
	private static LongComparator instance;
	private LongComparator() {}
	
	/**
	 * Return instance of comparator
	 * @return Instance of long comparator
	 */
	public static LongComparator getInstance() {
		if(instance == null) {
			instance = new LongComparator();
		}
		return instance;
	}

	@Override
	public int compare(Long o1, Long o2) {
		return o1.compareTo(o2);
	}

}
