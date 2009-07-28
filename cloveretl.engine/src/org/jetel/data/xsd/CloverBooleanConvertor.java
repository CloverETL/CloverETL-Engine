
package org.jetel.data.xsd;

import javax.xml.bind.DatatypeConverter;
import org.apache.log4j.Logger;
import org.jetel.exception.DataConversionException;

/**
 *
 * @author Pavel Pospichal
 */
public class CloverBooleanConvertor implements IGenericConvertor {

    private static Logger logger = Logger.getLogger(CloverBooleanConvertor.class);
    
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

    public Object parse(String input) throws DataConversionException {
        return parseXsdBooleanToBoolean(input);
    }

    public String print(Object obj) throws DataConversionException {
        if (!(obj instanceof Boolean)) {
            throw new DataConversionException("Unsupported type by convertion: " + obj.getClass().getName());
        }

        return printBooleanToXsdBoolean((Boolean) obj);
    }
}
