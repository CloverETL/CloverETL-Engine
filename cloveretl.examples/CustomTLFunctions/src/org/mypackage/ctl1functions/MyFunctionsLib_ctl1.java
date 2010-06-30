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
 
public class MyFunctionsLib_ctl1 extends TLFunctionLibrary {
 
	private static final String LIBRARY_NAME = "MyCTL1Functions";
 
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
 
    class MyFunction extends TLFunctionPrototype {
 
        public MyFunction() {
            super(LIBRARY_NAME, "my_function", "Description of my function",
            		new TLValueType[] { TLValueType.DATE, TLValueType.STRING }, //MyFunction1 has 2 input parameters: 1st is date type,2nd is string type
                    TLValueType.STRING);//MyFunction1 returns string
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
 
    class DoubleMetaphoneFunction extends TLFunctionPrototype {
 
        public DoubleMetaphoneFunction() {
            super(LIBRARY_NAME, "double_metaphone", "Encodes a given word using the Double Metaphone algorithm", 
            		new TLValueType[] { TLValueType.STRING}, 
                    TLValueType.STRING);
        }
 
        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
        	TLStringValue val = (TLStringValue)context.context;
            StringBuilder strBuf = (StringBuilder)val.getValue();
            strBuf.setLength(0);
  
            //checking input parameters
            if (params[0]==TLNullValue.getInstance()) {
                throw new TransformLangExecutorRuntimeException(params,
                        Function.DOUBLE_METAPHONE.name()+" - NULL value not allowed");
            }
            if (params[0].type!=TLValueType.STRING)
                throw new TransformLangExecutorRuntimeException(params,
                		Function.DOUBLE_METAPHONE.name() + " - wrong type of literal");
 
            String word = params[0].toString();
    		DoubleMetaphone doubleMetaphone = new DoubleMetaphone();
    		doubleMetaphone.setMaxCodeLen(word.length());
    		strBuf.append(doubleMetaphone.doubleMetaphone(word));

    		return val;
        }
 
        @Override
        public TLContext createContext() {
            return TLContext.createStringContext();
        }
    }
}
 
 
