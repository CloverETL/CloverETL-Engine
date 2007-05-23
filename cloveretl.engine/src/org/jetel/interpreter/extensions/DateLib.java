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

import org.jetel.data.primitive.CloverDouble;
import org.jetel.data.primitive.Numeric;
import org.jetel.interpreter.Stack;
import org.jetel.interpreter.TransformLangExecutorRuntimeException;
import org.jetel.interpreter.data.TLContext;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.util.StringUtils;

public class DateLib {

    public static void init() {

        // TODAY

        TLFunctionFactory.registerFunction(new TLFunctionPrototype("date",
                "today", new TLValueType[] { },
                TLValueType.DATE) {
            @Override
            public TLValue execute(TLValue[] params, TLContext context) {
                return new TLValue(TLValueType.DATE,Calendar.getInstance().getTime());
            }
        });
        
        
        
    }
}
