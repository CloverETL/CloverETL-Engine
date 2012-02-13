/*
 * Copyright 2006-2009 Opensys TM by Javlin, a.s. All rights reserved.
 * Opensys TM by Javlin PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * Opensys TM by Javlin a.s.; Kremencova 18; Prague; Czech Republic
 * www.cloveretl.com; info@cloveretl.com
 *
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
