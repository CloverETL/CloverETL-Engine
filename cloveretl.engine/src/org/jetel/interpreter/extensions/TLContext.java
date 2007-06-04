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
 * Created on 3.4.2007
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.interpreter.extensions;

import java.util.Date;

import org.jetel.data.primitive.CloverDouble;
import org.jetel.data.primitive.CloverInteger;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.metadata.DataFieldMetadata;

public class TLContext<T> {

    public T context;

    public void setContext(T ctx) {
        this.context = ctx;
    }

    public T getContext() {
        return context;
    }

    public static TLContext<TLValue> createStringContext() {
        TLContext<TLValue> context = new TLContext<TLValue>();
        context.setContext(new TLValue(TLValueType.STRING, new StringBuilder(40)));
        return context;
    }

    public static TLContext<TLValue> createIntegerContext() {
        TLContext<TLValue> context = new TLContext<TLValue>();
        context.setContext(new TLValue(TLValueType.INTEGER,new CloverInteger(0)));
        return context;
    }

    public static TLContext<TLValue> createDoubleContext() {
        TLContext<TLValue> context = new TLContext<TLValue>();
        context.setContext(new TLValue(TLValueType.DOUBLE,new CloverDouble(0)));
        return context;

    }

    public static TLContext<TLValue> createDateContext() {
        TLContext<TLValue> context = new TLContext<TLValue>();
        context.setContext(new TLValue(TLValueType.DATE,new Date(0)));
        return context;

    }
    
}
