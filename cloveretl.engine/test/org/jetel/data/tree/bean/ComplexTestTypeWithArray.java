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

import java.util.Arrays;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 25.10.2011
 */
public class ComplexTestTypeWithArray extends ComplexTestType {

	private SimpleTestType[][] simpleTypedValuesMatrix;
	
	private ComplexTestTypeWithArray anotheComplexTestTypeWithArray;

	public SimpleTestType[][] getSimpleTypedValuesMatrix() {
		return simpleTypedValuesMatrix;
	}

	public void setSimpleTypedValuesMatrix(SimpleTestType[][] simpleTypedValuesMatrix) {
		this.simpleTypedValuesMatrix = simpleTypedValuesMatrix;
	}

	public ComplexTestTypeWithArray getAnotheComplexTestTypeWithArray() {
		return anotheComplexTestTypeWithArray;
	}

	public void setAnotheComplexTestTypeWithArray(ComplexTestTypeWithArray anotheComplexTestTypeWithArray) {
		this.anotheComplexTestTypeWithArray = anotheComplexTestTypeWithArray;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((anotheComplexTestTypeWithArray == null) ? 0 : anotheComplexTestTypeWithArray.hashCode());
		result = prime * result + Arrays.hashCode(simpleTypedValuesMatrix);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!super.equals(obj)) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		
		ComplexTestTypeWithArray other = (ComplexTestTypeWithArray) obj;
		if (anotheComplexTestTypeWithArray == null) {
			if (other.anotheComplexTestTypeWithArray != null) {
				return false;
			}
		} else if (!anotheComplexTestTypeWithArray.equals(other.anotheComplexTestTypeWithArray)) {
			return false;
		}
		for (int i = 0; i < simpleTypedValuesMatrix.length; i++) {
			if (!Arrays.equals(simpleTypedValuesMatrix[i], other.simpleTypedValuesMatrix[i])) {
				return false;
			}
		}
		return true;
	}
	
	
}
