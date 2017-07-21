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
package org.jetel.data.tree.bean;

import java.util.Map;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 25.10.2011
 */
public class ComplexTestType {

	private Map<String, SimpleTestType> mapStringToSimpleType;
	
	private ComplexTestType otherComplexType;
	
	public ComplexTestType getOtherComplexType() {
		return otherComplexType;
	}

	public void setOtherComplexType(ComplexTestType otherComplexType) {
		this.otherComplexType = otherComplexType;
	}

	public Map<String, SimpleTestType> getMapStringToSimpleType() {
		return mapStringToSimpleType;
	}

	public void setMapStringToSimpleType(Map<String, SimpleTestType> mapStringToSimpleType) {
		this.mapStringToSimpleType = mapStringToSimpleType;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((mapStringToSimpleType == null) ? 0 : mapStringToSimpleType.hashCode());
		result = prime * result + ((otherComplexType == null) ? 0 : otherComplexType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		
		ComplexTestType other = (ComplexTestType) obj;
		if (mapStringToSimpleType == null) {
			if (other.mapStringToSimpleType != null) {
				return false;
			}
		} else if (!mapStringToSimpleType.equals(other.mapStringToSimpleType)) {
			return false;
		}
		if (otherComplexType == null) {
			if (other.otherComplexType != null) {
				return false;
			}
		} else if (!otherComplexType.equals(other.otherComplexType)) {
			return false;
		}
		
		return true;
	}
	
	
}
