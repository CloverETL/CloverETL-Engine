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
import org.jetel.data.DataField;
import org.jetel.data.DataFieldFactory;
import org.jetel.data.primitive.HugeDecimal;
import org.jetel.data.primitive.Numeric;
import org.jetel.metadata.DataFieldMetadata;

/**
 * Factory for creation of Numeric instances  based on specified data field type, which are suitable for computations of aggregate functions.
 * This class with Aggregate*Numeric classes where introduced to detect arithmetic overflows during computation of aggregation functions.
 * More precisely, only Avg and Sum functions where considered, and therefore only operation of addition was considered.
 *
 * @author tkramolis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4 Mar 2011
 */
public class AggregateNumericFactory {

	public final static Numeric createDataField(char fieldType, DataFieldMetadata fieldMetadata) {
		switch (fieldType) {
			case DataFieldMetadata.INTEGER_FIELD:
				return new AggregateIntegerNumeric(fieldMetadata);
			case DataFieldMetadata.LONG_FIELD:
				return new AggregateLongNumeric(fieldMetadata);
			case DataFieldMetadata.DECIMAL_FIELD:
				// DecimalDataField overflow workaround: always use HugeDecimal which uses BigDecimal which can't overflow
				return new HugeDecimal(null, fieldMetadata.getFieldProperties().getIntProperty(DataFieldMetadata.LENGTH_ATTR), fieldMetadata.getFieldProperties().getIntProperty(DataFieldMetadata.SCALE_ATTR), true);
			default:
				DataField field = DataFieldFactory.createDataField(fieldType, fieldMetadata, true);
				if (!(field instanceof Numeric)) {
					throw new RuntimeException("Unsupported numeric data type: " + fieldType);
				}
				return (Numeric) field;
		}
	}

	public final static Numeric createDataField(DataFieldMetadata fieldMetadata){
		return createDataField(fieldMetadata.getType(),fieldMetadata);
	}
	
}

