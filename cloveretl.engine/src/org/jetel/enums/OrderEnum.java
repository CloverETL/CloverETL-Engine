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
package org.jetel.enums;


/**
 * Order Enum.
 * 
 * @author Jan Ausperger (jan.ausperger@javlin.eu)
 * (c) Javlin, a.s. (www.javlin.eu)
 */
public enum OrderEnum {
	ASC("a", "ascending"),	// ascending order
	DESC("d", "descending"),// descending order
	IGNORE("i", "ignore"),	// don't check order of records
	AUTO("r", "auto");	    // check the input to be either in ascending or descending order

	String shortCut;
	String description;

	OrderEnum(String shortCut, String description) {
		this.description = description;
		this.shortCut = shortCut;
	}

	public static OrderEnum fromString(String id) {
		return fromString(id, null);
	}
	
	public static OrderEnum fromString(String id, OrderEnum defaultValue) {
        if(id == null) return defaultValue;
        
        for(OrderEnum item : values()) {
            if(id.equalsIgnoreCase(item.shortCut)) {
                return item;
            }
        }
        return defaultValue;
	}

	public String getShortCut() {
		return shortCut;
	}
	
	public String getDescription() {
		return description;
	}
	
}
