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
package org.mypackage.ctl1functions;
 
 
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.jetel.interpreter.TransformLangExecutorRuntimeException;
import org.jetel.interpreter.data.TLNullValue;
import org.jetel.interpreter.data.TLStringValue;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.interpreter.extensions.TLContext;
import org.jetel.interpreter.extensions.TLFunctionLibrary;
import org.jetel.interpreter.extensions.TLFunctionPrototype;
 
/**
 * This class represents user's CTL1 functions library.
 * The library contains two functions and one context. 
 * It must contain enum with functions listing and implement 
 * <i>getAllFunctions()</i> and <i>getFunction(String)</i> methods.
 * 
 * The names of functions must correspond with the names in 
 * <i>plugin.xml</i> file (see <i>ctl1_plugin.xml</i>, that is renamed 
 * to <i>plugin.xml</i> during installation - {@link readme.html})
 * 
 * @author Agata Vackova (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2010-07-01
 */
public class MyFunctionsLib_ctl1 extends TLFunctionLibrary {
 
	private static final String LIBRARY_NAME = "MyCTL1Functions";
 
	/**
	 * List of functions in the library
	 */
	enum Function {
        MY_FUNCTION("my_function"),
        DOUBLE_METAPHONE("double_metaphone");
 
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
 
	public MyFunctionsLib_ctl1(){
		super();
	}
 
    public TLFunctionPrototype getFunction(String functionName) {
        switch(Function.fromString(functionName)) {
        case MY_FUNCTION: return new MyFunction();
        case DOUBLE_METAPHONE: return new DoubleMetaphoneFunction();
        default: return null;
       }
    }
 
    /* (non-Javadoc)
	 * @see org.jetel.interpreter.extensions.ITLFunctionLibrary#getAllFunctions()
	 */
	public Collection<TLFunctionPrototype> getAllFunctions() {
    	List<TLFunctionPrototype> ret = new ArrayList<TLFunctionPrototype>();
    	Function[] fun = Function.values();
    	for (Function function : fun) {
    		ret.add(getFunction(function.name));
		}
 
    	return ret;
	}
 
    /**
     * Implementation of my_function function
     */
    class MyFunction extends TLFunctionPrototype {
 
        public MyFunction() {
            super(LIBRARY_NAME, "my_function", "Description of my function",
            		new TLValueType[] { TLValueType.DATE, TLValueType.STRING }, //my_function has 2 input parameters: 1st is date type,2nd is string type
                    TLValueType.STRING);//my_function returns string
        }
 
        @Override
        public TLValue execute(TLValue[] params, TLContext context) {//input parameters and context (the one that is created by createContext() method)
        	//because we use string context, context in context is TLStringValue
        	//we will fulfill the value by the result of our function
        	TLStringValue val = (TLStringValue)context.context;
            StringBuilder strBuf = (StringBuilder)val.getValue();
            strBuf.setLength(0);
 
            //checking input parameters' types
            if (params[0]==TLNullValue.getInstance() || params[1]==TLNullValue.getInstance()) {
                throw new TransformLangExecutorRuntimeException(params,
                        Function.MY_FUNCTION.name()+" - NULL value not allowed");
            }
            if (params[0].type!=TLValueType.DATE || params[1].type!=TLValueType.STRING)
                throw new TransformLangExecutorRuntimeException(params,
                		Function.MY_FUNCTION.name() + " - wrong type of literal");
 
            String result = "MyFunction result";
            //TODO fullfil the result
            strBuf.append(result);
            //val contains StringBuffer we have just fulfilled 
            return val;
        }
 
        @Override
        public TLContext createContext() {
        	//You can use one of pre-defined context or create your own context (see next example)
 
//        	return TLContext.createByteContext(); - contains TLByteArrayValue - with default initial size (INITIAL_BYTE_ARRAY_CAPACITY = 8)
//        	return TLContext.createDateContext(); - contains TLDateValue 
//        	return TLContext.createDoubleContext(); - contains TLNumericValue<CloverDouble> - used for storing double values
//        	return TLContext.createIntegerContext(); - contains TLNumericValue<CloverInteger> - used for storing integer values
//        	return TLContext.createListContext(); - contains TLListValue with empty list
//        	return TLContext.createLongContext(); - contains TLNumericValue<CloverLong> - used for storing long values
//        	return TLContext.createNullContext(); - empty context - can be used when there are no object for repeated usage
            return TLContext.createStringContext();//contains TLStringValue - StringBuffer can be re-used
        }
    }
 
	 
    /**
     * Implementation of double_metaphone function
     */
    class DoubleMetaphoneFunction extends TLFunctionPrototype {
    	
        public DoubleMetaphoneFunction() {
            super(LIBRARY_NAME, "double_metaphone", "Encodes a given word using the Double Metaphone algorithm", 
            		new TLValueType[] { TLValueType.STRING, TLValueType.INTEGER}, //first argument is of the type string, the second one - integer
                    TLValueType.STRING,//double_metaphone returns string value
                    2, 1);//accepts maximally 2 parameters, minimum one parameter must be present
        }
 
        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
        	//usage of custom context (see below)
        	DoubleMetaphoneContext con = (DoubleMetaphoneContext)context.context;
			TLStringValue val = (TLStringValue) con.value;
			StringBuilder strBuf = (StringBuilder)val .getValue();
            strBuf.setLength(0);
  
            //checking input parameters
            if (params[0]==TLNullValue.getInstance()) {
                throw new TransformLangExecutorRuntimeException(params,
                        Function.DOUBLE_METAPHONE.name()+" - NULL value not allowed");
            }
            if (params[0].type!=TLValueType.STRING)
                throw new TransformLangExecutorRuntimeException(params,
                		Function.DOUBLE_METAPHONE.name() + " - wrong type of literal");
            if (params.length > 1 && params[1].type!=TLValueType.INTEGER)
                throw new TransformLangExecutorRuntimeException(params,
                		Function.DOUBLE_METAPHONE.name() + " - wrong type of literal");
 
            String word = params[0].toString();
            //if exists 2nd parameter, use it as max code length
            int length = params.length > 1 ? params[1].getNumeric().getInt() : word.length();
            //checking context settings
    		if (length != con.maxCodeLength) {
				DoubleMetaphoneContext.doubleMetaphone.setMaxCodeLen(length);
			}
    		//"body" of the function
			strBuf.append(DoubleMetaphoneContext.doubleMetaphone.doubleMetaphone(word));

    		return val;
        }
 
        @Override
        public TLContext createContext() {
            return DoubleMetaphoneContext.createContex();
        }
    }
    
}
 
/**
 * This class demonstrates how to create your own context.
 * In context you should store objects that can be reused in every execution of your function.
 * This saves time when executing the same function many times.
 * 
 * @author Agata Vackova (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2010-07-01
 */
class DoubleMetaphoneContext {
	
	final static DoubleMetaphone doubleMetaphone = new DoubleMetaphone();
	int maxCodeLength = -1;
	TLValue value;
	
    static TLContext createContex(){
    	DoubleMetaphoneContext con=new DoubleMetaphoneContext();
        con.value=TLValue.create(TLValueType.STRING);
        TLContext<DoubleMetaphoneContext> context=new TLContext<DoubleMetaphoneContext>();
        context.setContext(con);
 
        return context;        	
    }
}

