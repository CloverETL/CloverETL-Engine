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
package org.jetel.component.aggregate;

import org.jetel.data.LongDataField;
import org.jetel.data.primitive.Numeric;
import org.jetel.metadata.DataFieldMetadata;

/**
 * Long integer {@link Numeric} based on {@link LongDataField} with additional check for arithmetic overflow in {@link #add(Numeric)} operation.
 *
 * @author tkramolis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4 Mar 2011
 */
public class AggregateLongNumeric extends LongDataField {

	private static final long serialVersionUID = 1041354863302055685L;

	public AggregateLongNumeric(DataFieldMetadata _metadata) {
		super(_metadata);
	}
	
	@Override
	public void add(Numeric a) {
        if (isNull) return;
        if (a.isNull()) {
            setNull(true);
        } else {
			long paramValue = a.getLong();
			long value = getLong();
			long sum = value + paramValue;
			if ((value > 0 && paramValue > 0 && sum < 0) || (value < 0 && paramValue < 0 && sum > 0) || sum == Long.MIN_VALUE) {
				throw new ArithmeticException("Integer overflow (" + value + " + " + paramValue + " = " + sum + ")");
			}
			setValue(sum);
        }
	}
	
}
