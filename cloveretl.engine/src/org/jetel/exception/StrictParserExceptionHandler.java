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


/**
 * Parser exception handler with type "strict". Handled exception is only re-throwed.
 * 
 * @author Martin Zatopek, Javlin Consulting (www.javlinconsulting.cz)
 *
 */
public class StrictParserExceptionHandler extends AbstractParserExceptionHandler {

    private PolicyType type;

    public StrictParserExceptionHandler() {
        this(PolicyType.STRICT);
    }
    
    public StrictParserExceptionHandler(PolicyType type) {
        this.type = type;
    }
    
    @Override
    protected void handle() {
    	BadDataFormatException ex = exception;
    	exception = null;
    	if(ex.getOffendingValue() == null && getRawRecord() != null) {
    		ex.setOffendingValue(getRawRecord());
    	}
    	ex.setRawRecord(getRawRecord());
        throw ex;
    }

    @Override
    public PolicyType getType() {
        return type;
    }

}
