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
package org.jetel.exception;

import org.jetel.component.RecordTransform;

/**
 * Exception is thrown in custom transformation function.
 * @see RecordTransform
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 9.11.2006
 */
public class TransformException extends JetelException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private int recNo = -1;
    private int fieldNo = -1;

    /**
	 * @param message
	 * @param recNo
	 * @param fieldNo
	 */
	public TransformException(String message, int recNo, int fieldNo) {
		super(message);
		this.recNo = recNo;
		this.fieldNo = fieldNo;
	}
	
	public TransformException(String message, Throwable cause, int recNo, int fieldNo) {
		super(message, cause);
		this.recNo = recNo;
		this.fieldNo = fieldNo;
	}


	public TransformException(String message) {
        super(message);
    }

    public TransformException(String message, Throwable cause) {
        super(message, cause);
    }

	public TransformException(Throwable cause) {
		super(cause);
	}

	public int getFieldNo() {
		return fieldNo;
	}

	public void setFieldNo(int fieldNo) {
		this.fieldNo = fieldNo;
	}

	public int getRecNo() {
		return recNo;
	}

	public void setRecNo(int recNo) {
		this.recNo = recNo;
	}

	@Override
	public String getMessage() {
		StringBuilder msg = new StringBuilder();
		if (recNo > -1) {
			msg.append("Record number: ");
			msg.append(recNo);
			if (fieldNo > -1) {
				msg.append(", field number: ");
				msg.append(fieldNo);
			}
			msg.append(". ");
		} 
		msg.append("Message: ");
		msg.append(super.getMessage());
		return msg.toString();
	}
    
}
