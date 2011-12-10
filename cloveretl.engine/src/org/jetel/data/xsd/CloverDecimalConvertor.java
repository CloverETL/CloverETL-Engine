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
package org.jetel.data.xsd;

import java.math.BigDecimal;
import javax.xml.bind.DatatypeConverter;
import org.apache.log4j.Logger;
import org.jetel.exception.DataConversionException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.data.primitive.Numeric;

/**
 *
 * @author Pavel Pospichal
 */
public class CloverDecimalConvertor implements IGenericConvertor {

    private static Logger logger = Logger.getLogger(CloverDecimalConvertor.class);
    
    static {
    	ConvertorRegistry.registerConvertor(new CloverDecimalConvertor());
    }
    
    public static Numeric parseXsdDecimalToNumeric(String value) throws DataConversionException {
        Numeric result = null;
        String valueType = Numeric.class.getName();
        
        try {
            BigDecimal decimal = DatatypeConverter.parseDecimal(value);
            result = DecimalFactory.getDecimal(decimal);
        } catch(Exception e) {
            if (result != null) valueType = result.getClass().getName();
            logger.fatal("Unable to parse xsd:decimal to "+valueType+".",e);
            throw new DataConversionException("Unable to parse xsd:decimal to "+valueType+".", e);
        }
        
        return result;
    }
    
    public static String printNumericToXsdDecimal(Numeric value) throws DataConversionException {
        String result = null;
        String valueType = Numeric.class.getName();
        
        try {
            BigDecimal decimal = value.getBigDecimal();
            result = DatatypeConverter.printDecimal(decimal);
        } catch(Exception e) {
            if (value != null) valueType = value.getClass().getName();
            logger.fatal("Unable to print "+valueType+" to xsd:decimal.",e);
            throw new DataConversionException("Unable to print "+valueType+" to xsd:decimal.", e);
        }
        
        return result;
    }

    @Override
	public Object parse(String input) throws DataConversionException {
        return parseXsdDecimalToNumeric(input);
    }

    @Override
	public String print(Object obj) throws DataConversionException {
        if (!(obj instanceof Numeric)) {
            throw new DataConversionException("Unsupported type by convertion: " + obj.getClass().getName());
        }

        return printNumericToXsdDecimal((Numeric) obj);
    }

	@Override
	public boolean supportsCloverType(String cloverDataTypeCriteria) {
		return DataFieldMetadata.DECIMAL_TYPE.equals(cloverDataTypeCriteria);
	}
	
	@Override
	public boolean supportsExternalSystemType(String externalTypeCriteria) {
		return true;
	}
}
