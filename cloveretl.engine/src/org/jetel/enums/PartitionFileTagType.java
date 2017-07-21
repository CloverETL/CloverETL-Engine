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
 * This enum represents type of partition file tag as reaction on BadDataFormatException in parses.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public enum PartitionFileTagType {

	NUMBER_FILE_TAG("numberFileTag"), KEY_NAME_FILE_TAG("keyNameFileTag");

	private String id;
	
	private PartitionFileTagType(String id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return id;
	}
	
    public static PartitionFileTagType valueOfIgnoreCase(String partitionFileTagType) {
        if(partitionFileTagType == null) {
            return NUMBER_FILE_TAG;
        }
        
        for(PartitionFileTagType pt : PartitionFileTagType.values()) {
            if(partitionFileTagType.equalsIgnoreCase(pt.id)) {
                return pt;
            }
        }
        
        return NUMBER_FILE_TAG; //default value
    }
}
