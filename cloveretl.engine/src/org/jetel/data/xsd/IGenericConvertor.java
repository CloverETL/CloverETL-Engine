
package org.jetel.data.xsd;

import org.jetel.exception.DataConversionException;

/**
 *
 * @author Pavel Pospichal
 */
public interface IGenericConvertor {

    public Object parse(String input) throws DataConversionException;
    public String print(Object obj) throws DataConversionException;
    public boolean supportsCloverType(String cloverDataTypeCriteria);
}
