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

import javax.xml.bind.DatatypeConverter;
import org.apache.log4j.Logger;
import org.jetel.exception.DataConversionException;
import org.jetel.metadata.DataFieldMetadata;

/**
 *
 * @author Pavel Pospichal
 */
public class CloverBooleanConvertor implements IGenericConvertor {

    private static Logger logger = Logger.getLogger(CloverBooleanConvertor.class);
    
    static {
    	ConvertorRegistry.registerConvertor(new CloverBooleanConvertor());
    }
    
    public static Boolean parseXsdBooleanToBoolean(String value) throws DataConversionException {
        Boolean result = null;
        try {
            result = DatatypeConverter.parseBoolean(value);    
        } catch(Exception e) {
            logger.fatal("Unable to parse xsd:boolean to "+Boolean.class.getName()+".",e);
            throw new DataConversionException("Unable to parse xsd:boolean to "+Boolean.class.getName()+".", e);
        }
        
        return result;
    }
    
    public static String printBooleanToXsdBoolean(Boolean value) throws DataConversionException {
        String result = null;
        try {
           result = DatatypeConverter.printBoolean(value);
        } catch(Exception e) {
            logger.fatal("Unable to print "+Boolean.class.getName()+" to xsd:boolean.",e);
            throw new DataConversionException("Unable to print "+Boolean.class.getName()+" to xsd:boolean.", e);
        }
        
        return result;
    }

    @Override
	public Object parse(String input) throws DataConversionException {
        return parseXsdBooleanToBoolean(input);
    }

    @Override
	public String print(Object obj) throws DataConversionException {
        if (!(obj instanceof Boolean)) {
            throw new DataConversionException("Unsupported type by convertion: " + obj.getClass().getName());
        }

        return printBooleanToXsdBoolean((Boolean) obj);
    }

	@Override
	public boolean supportsCloverType(String cloverDataTypeCriteria) {
		return DataFieldMetadata.BOOLEAN_TYPE.equals(cloverDataTypeCriteria);
	}

	@Override
	public boolean supportsExternalSystemType(String externalTypeCriteria) {
		return true;
	}
}
