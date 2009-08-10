
package org.jetel.data.xsd;

import javax.xml.bind.DatatypeConverter;
import org.apache.log4j.Logger;
import org.jetel.exception.DataConversionException;
import org.jetel.metadata.DataFieldMetadata;

/**
 *
 * @author Pavel Pospichal
 */
public class CloverByteArrayConvertor implements IGenericConvertor {

    private static Logger logger = Logger.getLogger(CloverByteArrayConvertor.class);
    
    static {
    	ConvertorRegistry.registerConvertor(new CloverByteArrayConvertor());
    }
    
	public static byte[] parseXsdBase64ToByteArray(String value) throws DataConversionException {
		byte[] result = null;
		try {
			result = DatatypeConverter.parseBase64Binary(value);
		} catch (Exception e) {
			logger.fatal("Unable to parse xsd:base64Binary to " + byte[].class.getName() + ".", e);
			throw new DataConversionException("Unable to parse xsd:base64Binary to " + byte[].class.getName() + ".", e);
		}

		return result;
	}
    
	public static String printByteArrayToXsdBase64(byte[] value) throws DataConversionException {
		String result = null;
		try {
			result = DatatypeConverter.printBase64Binary(value);
		} catch (Exception e) {
			logger.fatal("Unable to print " + byte[].class.getName() + " to xsd:base64Binary.", e);
			throw new DataConversionException("Unable to print " + byte[].class.getName() + " to xsd:base64Binary.", e);
		}

		return result;
	}

	public Object parse(String input) throws DataConversionException {
		return parseXsdBase64ToByteArray(input);
	}

	public String print(Object obj) throws DataConversionException {
		if (!(obj instanceof byte[])) {
			throw new DataConversionException("Unsupported type by convertion: " + obj.getClass().getName());
		}

		return printByteArrayToXsdBase64((byte[]) obj);
	}

	public boolean supportsCloverType(String cloverDataTypeCriteria) {
		return DataFieldMetadata.BYTE_TYPE.equals(cloverDataTypeCriteria) || 
		DataFieldMetadata.BYTE_COMPRESSED_TYPE.equals(cloverDataTypeCriteria);
	}
}
