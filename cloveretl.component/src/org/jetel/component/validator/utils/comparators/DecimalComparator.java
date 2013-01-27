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

import org.jetel.data.primitive.Decimal;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 15.1.2013
 */
public class DecimalComparator implements Comparator<Decimal>{

	private static DecimalComparator instance;
	private DecimalComparator() {}
	
	public static DecimalComparator getInstance() {
		if(instance == null) {
			instance = new DecimalComparator();
		}
		return instance;
	}
	
	@Override
	public int compare(Decimal o1, Decimal o2) {
		return o1.compareTo(o2);
	}

}
