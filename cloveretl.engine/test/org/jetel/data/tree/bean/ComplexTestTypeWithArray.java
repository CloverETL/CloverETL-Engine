/*
 * Copyright 2006-2009 Opensys TM by Javlin, a.s. All rights reserved.
 * Opensys TM by Javlin PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * Opensys TM by Javlin a.s.; Kremencova 18; Prague; Czech Republic
 * www.cloveretl.com; info@cloveretl.com
 *
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
