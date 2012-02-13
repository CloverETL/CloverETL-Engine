/*
 * Copyright 2006-2009 Opensys TM by Javlin, a.s. All rights reserved.
 * Opensys TM by Javlin PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * Opensys TM by Javlin a.s.; Kremencova 18; Prague; Czech Republic
 * www.cloveretl.com; info@cloveretl.com
 *
 */

package org.jetel.data.tree.bean;

import java.util.List;
import java.util.Map;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 25.10.2011
 */
public class ComplexTestTypeWithCollection extends ComplexTestType {

	private List<Map<SimpleTestType, Double>> mapList;

	public List<Map<SimpleTestType, Double>> getMapList() {
		return mapList;
	}

	public void setMapList(List<Map<SimpleTestType, Double>> mapList) {
		this.mapList = mapList;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((mapList == null) ? 0 : mapList.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!super.equals(obj)){
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		
		ComplexTestTypeWithCollection other = (ComplexTestTypeWithCollection) obj;
		if (mapList == null) {
			if (other.mapList != null) {
				return false;
			}
		} else if (!mapList.equals(other.mapList)) {
			return false;
		}
		return true;
	}
	
	
}
