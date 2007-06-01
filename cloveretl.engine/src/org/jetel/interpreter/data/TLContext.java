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
package org.jetel.interpreter.data;

import org.jetel.data.primitive.CloverDouble;
import org.jetel.data.primitive.CloverInteger;

public class TLContext<T> {

    public T context;

    public void setContext(T ctx) {
        this.context = ctx;
    }

    public T getContext() {
        return context;
    }

    public static TLContext<StringBuilder> createStringContext() {
        TLContext<StringBuilder> context = new TLContext<StringBuilder>();
        context.setContext(new StringBuilder(40));
        return context;
    }

    public static TLContext<CloverInteger> createIntegerContext() {
        TLContext<CloverInteger> context = new TLContext<CloverInteger>();
        context.setContext(new CloverInteger(0));
        return context;
    }

    public static TLContext<CloverDouble> createDoubleContext() {
        TLContext<CloverDouble> context = new TLContext<CloverDouble>();
        context.setContext(new CloverDouble(0));
        return context;

    }

}
