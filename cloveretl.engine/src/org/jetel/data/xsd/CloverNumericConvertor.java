
package org.jetel.data.xsd;

import javax.xml.bind.DatatypeConverter;
import org.apache.log4j.Logger;
import org.jetel.exception.DataConversionException;
import org.jetel.metadata.DataFieldMetadata;

/**
 *
 * @author Pavel Pospichal
 */
public class CloverNumericConvertor implements IGenericConvertor {

    private static Logger logger = Logger.getLogger(CloverNumericConvertor.class);
    
    static {
    	ConvertorRegistry.registerConvertor(new CloverNumericConvertor());
    }
    
    public static Double parseXsdDoubleToDouble(String value) throws DataConversionException {
        Double result = null;
        
        try {
            result = DatatypeConverter.parseDouble(value);
        } catch(Exception e) {
            logger.fatal("Unable to parse xsd:double to "+Double.class.getName()+".",e);
            throw new DataConversionException("Unable to parse xsd:double to "+Double.class.getName()+".", e);
        }
        
        return result;
    }
    
    public static String printDoubleToXsdDouble(Double value) throws DataConversionException {
        String result = null;
        
        try {
            result = DatatypeConverter.printDouble(value);
        } catch(Exception e) {
            logger.fatal("Unable to print "+Double.class.getName()+" to xsd:double.",e);
            throw new DataConversionException("Unable to print "+Double.class.getName()+" to xsd:double.", e);
        }
        
        return result;
    }

    public Object parse(String input) throws DataConversionException {
        return parseXsdDoubleToDouble(input);
    }

    public String print(Object obj) throws DataConversionException {
        if (!(obj instanceof Double)) {
            throw new DataConversionException("Unsupported type by convertion: " + obj.getClass().getName());
        }

        return printDoubleToXsdDouble((Double) obj);
    }

	public boolean supportsCloverType(String cloverDataTypeCriteria) {
		return DataFieldMetadata.NUMERIC_TYPE.equals(cloverDataTypeCriteria);
	}
	
	public boolean supportsExternalSystemType(String externalTypeCriteria) {
		return true;
	}
}
