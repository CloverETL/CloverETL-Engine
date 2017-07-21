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
package org.jetel.ctl.extensions;

import java.util.UUID;

import org.jetel.ctl.Stack;

public class UtilLib extends TLFunctionLibrary {

    @Override
    public TLFunctionPrototype getExecutable(String functionName) {
    	final TLFunctionPrototype ret = 
    		"sleep".equals(functionName) ? new SleepFunction() :
    		"randomUUID".equals(functionName) ? new RandomUuidFunction() : null; 
    		
		if (ret == null) {
    		throw new IllegalArgumentException("Unknown function '" + functionName + "'");
    	}
    
		return ret;
    }
    
	private static String LIBRARY_NAME = "Util";

	@Override
	public String getName() {
		return LIBRARY_NAME;
	}


    // SLEEP
    @TLFunctionAnnotation("Pauses execution for specified milliseconds")
    public static final void sleep(TLFunctionCallContext context, long millis) {
    	try {
    		Thread.sleep(millis);
    	} catch (InterruptedException e) {
		}
    }

    class SleepFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			long millis = stack.popLong();
			sleep(context, millis);
		}
    	
    }
    
    // UUID
    @TLFunctionAnnotation("Generates random universally unique identifier (UUID)")
    public static String randomUUID(TLFunctionCallContext context) {
    	return UUID.randomUUID().toString();
    }
    
    class RandomUuidFunction implements TLFunctionPrototype {
    	
    	@Override
    	public void init(TLFunctionCallContext context) {
    	}
    	
    	@Override
    	public void execute(Stack stack, TLFunctionCallContext context) {
    		stack.push(randomUUID(context));
    	}
    }
}
