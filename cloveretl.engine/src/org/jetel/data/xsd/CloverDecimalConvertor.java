
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

    public Object parse(String input) throws DataConversionException {
        return parseXsdDecimalToNumeric(input);
    }

    public String print(Object obj) throws DataConversionException {
        if (!(obj instanceof Numeric)) {
            throw new DataConversionException("Unsupported type by convertion: " + obj.getClass().getName());
        }

        return printNumericToXsdDecimal((Numeric) obj);
    }

	public boolean supportsCloverType(String cloverDataTypeCriteria) {
		return DataFieldMetadata.DECIMAL_TYPE.equals(cloverDataTypeCriteria);
	}
	
	public boolean supportsExternalSystemType(String externalTypeCriteria) {
		return true;
	}
}
