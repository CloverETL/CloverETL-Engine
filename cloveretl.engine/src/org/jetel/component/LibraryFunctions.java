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
package org.jetel.component;

import java.util.Calendar;
import java.util.Date;

import org.jetel.util.DataGenerator;

/**
 * @author kubosj (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 9.2.2012
 */
public class LibraryFunctions {
	
	private final DataGenerator generator = new DataGenerator();
	private final Calendar calendar = Calendar.getInstance();
	
	public String randomString(int minLength, int maxLength) {
		return generator.nextString(minLength, maxLength);
	}
	
	public Date dateAdd(Date date, int amount, String unit) {
		calendar.setTime(date);
		calendar.add(unitToEnum(unit), amount);
		
		return calendar.getTime();
	}
	
	public long dateDiff(Date date1, Date date2, String unit) {
		if (!unit.equals("second")) {
			throw new RuntimeException("Unexpected unit "+unit);
		}
		
		return (date1.getTime()-date2.getTime())/1000;
	}
	
	private int unitToEnum(String unit) {
		if (unit.equals("day")) {
			return Calendar.DAY_OF_MONTH;
		} else if (unit.equals("second")) {
			return Calendar.SECOND;
		} else {
			throw new RuntimeException("Unexpected unit "+unit);
		}
	}
}
