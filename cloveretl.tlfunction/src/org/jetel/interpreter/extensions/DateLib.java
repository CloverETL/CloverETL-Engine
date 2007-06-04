/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-07  David Pavlis <david.pavlis@centrum.cz> and others.
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
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Created on 2.4.2007
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.interpreter.extensions;

import java.util.Calendar;

import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.interpreter.extensions.StringLib.Function;

public class DateLib extends TLFunctionLibrary {

    private static final String LIBRARY_NAME = "Date";

    enum Function {
        TODAY("today");
        
        public String name;
        
        private Function(String name) {
            this.name = name;
        }
        
        public static Function fromString(String s) {
            for(Function function : Function.values()) {
                if(s.equalsIgnoreCase(function.name) || s.equalsIgnoreCase(LIBRARY_NAME + "." + function.name)) {
                    return function;
                }
            }
            return null;
        }
    }

    public DateLib() {
        super();
     }

    public TLFunctionPrototype getFunction(String functionName) {
        switch(Function.fromString(functionName)) {
        case TODAY: return new TodayFunction();
        default: return null;
       }
    }

    // TODAY
    class TodayFunction extends TLFunctionPrototype {
    	
    	public TodayFunction(){
    		super("date", "today", new TLValueType[] { }, TLValueType.DATE);
    	}
    
        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val=((TLValue)context.getContext());
            val.getDate().setTime(Calendar.getInstance().getTimeInMillis()); 
            return  val;
        }
        
        @Override
        public TLContext createContext() {
            return TLContext.createDateContext();
        }
    }
    
}
