
package org.jetel.data.xsd;

import javax.xml.bind.DatatypeConverter;
import org.apache.log4j.Logger;
import org.jetel.exception.DataConversionException;
import org.jetel.metadata.DataFieldMetadata;

/**
 *
 * @author Pavel Pospichal
 */
public class CloverStringConvertor implements IGenericConvertor {

    private static Logger logger = Logger.getLogger(CloverStringConvertor.class);
    
    static {
    	ConvertorRegistry.registerConvertor(new CloverStringConvertor());
    }
    
    public static String parseXsdStringToString(String value) throws DataConversionException {
        String result = null;
        
        try {
            result = DatatypeConverter.parseString(value);    
        } catch(Exception e) {
            logger.fatal("Unable to parse xsd:string to "+String.class.getName()+".",e);
            throw new DataConversionException("Unable to parse xsd:string to "+String.class.getName()+".", e);
        }
        
        return result;
    }
    
    public static String printStringToXsdString(CharSequence value) throws DataConversionException {
        String result = null;
        String valueType = CharSequence.class.getName();
        
        try {
            String valueString = value.toString();
            result = DatatypeConverter.printString(valueString);
        } catch(Exception e) {
            if (value != null) valueType = value.getClass().getName();
            logger.fatal("Unable to print "+valueType+" to xsd:string.",e);
            throw new DataConversionException("Unable to print "+valueType+" to xsd:string.", e);
        }
        
        return result;
    }

    public Object parse(String input) throws DataConversionException {
        return parseXsdStringToString(input);
    }

    public String print(Object obj) throws DataConversionException {
        if (!(obj instanceof CharSequence)) {
            throw new DataConversionException("Unsupported type by convertion: " + obj.getClass().getName());
        }

        return printStringToXsdString((CharSequence) obj);
    }
    
	public boolean supportsCloverType(String cloverDataTypeCriteria) {
		return DataFieldMetadata.STRING_TYPE.equals(cloverDataTypeCriteria);
	}
}
