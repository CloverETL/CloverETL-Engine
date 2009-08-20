
package org.jetel.data.xsd;

import javax.xml.bind.DatatypeConverter;
import org.apache.log4j.Logger;
import org.jetel.exception.DataConversionException;
import org.jetel.metadata.DataFieldMetadata;

/**
 *
 * @author Pavel Pospichal
 */
public class CloverLongConvertor implements IGenericConvertor {

    private static Logger logger = Logger.getLogger(CloverLongConvertor.class);
    
    static {
    	ConvertorRegistry.registerConvertor(new CloverLongConvertor());
    }
    
    public static Long parseXsdLongToLong(String value) throws DataConversionException {
    	Long result = null;
        
		try {
			result = DatatypeConverter.parseLong(value);
		} catch (Exception e) {
			logger.fatal("Unable to parse xsd:long to " + Long.class.getName() + ".", e);
			throw new DataConversionException("Unable to parse xsd:long to " + Long.class.getName() + ".", e);
		}
        
        return result;
    }
    
    public static String printLongToXsdLong(Long value) throws DataConversionException {
        String result = null;
        
        try {
			result = DatatypeConverter.printLong(value);
		} catch (Exception e) {
			logger.fatal("Unable to print " + Long.class.getName() + " to xsd:long.", e);
			throw new DataConversionException("Unable to print " + Long.class.getName() + " to xsd:long.", e);
        }
        
        return result;
    }

    public Object parse(String input) throws DataConversionException {
        return parseXsdLongToLong(input);
    }

    public String print(Object obj) throws DataConversionException {
        if (!(obj instanceof Long)) {
            throw new DataConversionException("Unsupported type by convertion: " + obj.getClass().getName());
        }

        return printLongToXsdLong((Long) obj);
    }

	public boolean supportsCloverType(String cloverDataTypeCriteria) {
		return DataFieldMetadata.LONG_TYPE.equals(cloverDataTypeCriteria);
	}
	
	public boolean supportsExternalSystemType(String externalTypeCriteria) {
		return true;
	}
}
