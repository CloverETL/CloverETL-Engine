/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (c) Opensys TM by Javlin, a.s. (www.opensys.com)
 *   
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *   
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU   
 *    Lesser General Public License for more details.
 *   
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 *
 */
package org.jetel.component.infobright;

import java.nio.ByteBuffer;

import com.infobright.etl.model.ValueConverter;
import com.infobright.etl.model.ValueConverterException;
import com.infobright.etl.model.datatype.AbstractColumnType;


/**
 * @author avackova (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4 Nov 2009
 */
public class BooleanType extends AbstractColumnType {

	  private Boolean data;
	  private static final byte trueValue = '1';
	  private static final byte falseValue = '0';
	  
	  @Override
	  public String getDataAsString() {
	    return data == null ? "" : (data ? "1" : "0");
	  }

	  @Override
	  public void getData(ByteBuffer byteBuffer) {
	    byteBuffer.put(data ? trueValue : falseValue);
	  }
	  
	  @Override
	  public void setData(ByteBuffer byteBuffer) throws InvalidDataException {
	    data = byteBuffer.get() == trueValue;
	  }

	  @Override
	  public void setData(String string) {
	    try {
			data = Integer.parseInt(string) == 1;
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			data = null;
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
