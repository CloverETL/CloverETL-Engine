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
import org.jetel.ctl.extensions.TLCache;
import org.jetel.ctl.extensions.TLFunctionAnnotation;
import org.jetel.ctl.extensions.TLFunctionCallContext;
import org.jetel.ctl.extensions.TLFunctionLibrary;
import org.jetel.ctl.extensions.TLFunctionPrototype;

 /**
 * This class represents user's CTL2 functions library.
 * The library contains two functions and one cache (really simple - 
 * just for illustration how to use cache). 
 * It must implement <i>getExecutable(String)</i> method, that
 * defines custom functions. This is the only place, the functions
 * names are defined (in contrast to CTL1, where the names of functions 
 * must correspond with the names in <i>plugin.xml</i> file).
 * 
 * @author Agata Vackova (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2010-07-01
 */
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
			//arguments must be pop from stack in inverted order, that there were pushed on stack:
			//calling function(arg1, ..., argN) pushes arguments on stack: arg1, then arg2, ..., argN
			//so poping them must be in inverted order: argN, ..., arg1
			String arg2 = stack.popString();
			Date arg1 = stack.popDate();
			//pushing function result to executor stack
			stack.push(myFunction(context, arg1, arg2));
		}
	}

	private static DoubleMetaphone doubleMetaphone = new DoubleMetaphone();

	//Double metaphone with one argument
	@TLFunctionAnnotation("Encodes a given word using the Double Metaphone algorithm.")
	public static final String doubleMetaphone(TLFunctionCallContext context, String word) {
		return doubleMetaphone.doubleMetaphone(word);
	}

	//Double metaphone with two arguments
	@TLFunctionAnnotation("Encodes a given word using the Double Metaphone algorithm with defined max length.")
	public static final String doubleMetaphone(TLFunctionCallContext context, String word, int length) {
		return doubleMetaphone.doubleMetaphone(word);
	}

	// Double metaphone
	class DoubleMetaphoneFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
			//cache for some values that can be reused
			context.setCache(new DoubleMetaphoneCache());
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			int length = -1;
			//get second parameter if exist
			if (context.getParams().length > 1) {
				length = stack.popInt();
			}
			//get first parameter
			String word = stack.popString();
			//check and eventually change length
			if (length == -1) {
				length = word.length();
			}
			if (length != ((DoubleMetaphoneCache)context.getCache()).length) {
				doubleMetaphone.setMaxCodeLen(length);
				((DoubleMetaphoneCache)context.getCache()).length = length;
			}
			//push function result to executor stack
			stack.push(doubleMetaphone(context, word));
		}
	}

}
/**
 * Cache for the values, that can be reused,
 * when executing the same function many times.
 *  
 * @author Agata Vackova (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2 Jul 2010
 */
class DoubleMetaphoneCache extends TLCache {
	
	int length = -1;
	
}