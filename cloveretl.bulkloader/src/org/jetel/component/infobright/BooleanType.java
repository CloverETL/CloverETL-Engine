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
package org.jetel.component.infobright;

import java.nio.ByteBuffer;

import org.jetel.util.string.StringUtils;

import com.infobright.etl.model.ValueConverter;
import com.infobright.etl.model.ValueConverterException;
import com.infobright.etl.model.datatype.AbstractColumnType;


/**
 * @author avackova (info@cloveretl.com)
 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
 *
 * @created 4 Nov 2009
 */
public class BooleanType extends AbstractColumnType {

	  private Boolean data;

	  private static final byte TRUE_VALUE = 1;
	  private static final byte FALSE_VALUE = 0;
	  
	  private static final String TRUE_VALUE_STRING = "1";
	  private static final String FALSE_VALUE_STRING = "0";	  
	  
	  @Override
	  public String getDataAsString() {
	    return data == null ? "" : (data ? TRUE_VALUE_STRING : FALSE_VALUE_STRING);
	  }

	  @Override
	  public void getData(ByteBuffer byteBuffer) {
	    byteBuffer.put(data ? TRUE_VALUE : FALSE_VALUE);
	  }
	  
	  @Override
	  public void setData(ByteBuffer byteBuffer) throws InvalidDataException {
	    data = byteBuffer.get() == TRUE_VALUE;
	  }

	  @Override
	  public void setData(String string) {
		if (StringUtils.isEmpty(string)) {
			data = null;
			return;
		}
    	int val = 0;
		try {
			val = Integer.parseInt(string);
		} catch (NumberFormatException e) {
			throw new ValueConverterException("Value " + StringUtils.quote(string) + 
			" is not a boolean value. Only \"0\" and \"1\" can be converted to boolean.");
		}
    	switch (val) {
		case 0:
			data = false;
			break;
		case 1:
			data = true;
			break;
		default:
			throw new ValueConverterException("Value " + StringUtils.quote(string) + 
					" is not a boolean value. Only \"0\" and \"1\" can be converted to boolean.");
		}
	  }

	  @Override
	  protected void zeroOutData() {
	    data = null;
	  }
	  
	  @Override
	  public void setData(Object value, ValueConverter meta)   throws ValueConverterException {
	    Boolean val = meta.getBoolean(value);
	    if (val == null) {
	      setIsNull(true);
	    } else {
	      setIsNull(false);
	      data = val;
	    }
	  }
	  
	  @Override
	  public final boolean isNeedsEnclosures() {
	    return false;
	  }
}
