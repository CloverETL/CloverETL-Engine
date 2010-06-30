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
package org.mypackage.ctl2functions;

import java.util.Date;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.jetel.ctl.Stack;
import org.jetel.ctl.extensions.TLFunctionAnnotation;
import org.jetel.ctl.extensions.TLFunctionCallContext;
import org.jetel.ctl.extensions.TLFunctionLibrary;
import org.jetel.ctl.extensions.TLFunctionPrototype;


public class MyFunctionsLib_ctl2 extends TLFunctionLibrary {
	
	@Override
	public TLFunctionPrototype getExecutable(String functionName) {
		TLFunctionPrototype ret = 
			"myFunction".equals(functionName) ? new MyFunctionFunction() :
			"doubleMetaphone".equals(functionName) ? new DoubleMetaphoneFunction() : null;
			
		if (ret == null) {
			throw new IllegalArgumentException("Unknown function '" + functionName + "'");
		}

		return ret;
	}
	
	private static String LIBRARY_NAME = "MyCTL2Functions";

	public String getName() {
		return LIBRARY_NAME;
	}


	@TLFunctionAnnotation("Description of my function.")
	public static final String myFunction(TLFunctionCallContext context, Date arg1, String arg2) {
		return "MyFunction result";
	}

	// My function
	class MyFunctionFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			Date arg1 = stack.popDate();
			String arg2 = stack.popString();
			stack.push(myFunction(context, arg1, arg2));
		}
	}

	@TLFunctionAnnotation("Encodes a given word using the Double Metaphone algorithm.")
	public static final String doubleMetaphone(TLFunctionCallContext context, String word) {
		DoubleMetaphone doubleMetaphone = new DoubleMetaphone();
		doubleMetaphone.setMaxCodeLen(word.length());
		return doubleMetaphone.doubleMetaphone(word);
	}

	// Double metaphone
	class DoubleMetaphoneFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			String word = stack.popString();
			stack.push(doubleMetaphone(context, word));
		}
	}

}
