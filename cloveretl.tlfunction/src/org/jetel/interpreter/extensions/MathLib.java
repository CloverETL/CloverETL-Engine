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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.jetel.interpreter.TransformLangExecutorRuntimeException;
import org.jetel.interpreter.data.TLNumericValue;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;

public class MathLib extends TLFunctionLibrary {
    
    private static final String LIBRARY_NAME = "Math";
    
    enum Function {
        SQRT("sqrt"),
        LOG("log"),
        LOG10("log10"),
        EXP("exp"),
        ROUND("round"),
        POW("pow"),
        PI("pi"),
        E("e"),
        RANDOM("random"),
        ABS("abs");
        
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
    
    public  Collection<TLFunctionPrototype> getAllFunctions() {
    	List<TLFunctionPrototype> ret = new ArrayList<TLFunctionPrototype>();
    	Function[] fun = Function.values();
    	for (Function function : fun) {
    		ret.add(getFunction(function.name));
		}
    	
    	return ret;
    }
    
    public MathLib() {
        super();
        
    }

    public TLFunctionPrototype getFunction(String functionName) {
        switch(Function.fromString(functionName)) {
        case SQRT: return new SqrtFunction();
        case LOG: return new LogFunction();
        case LOG10: return new Log10Function();
        case EXP: return new ExpFunction();
        case ROUND: return new RoundFunction();
        case POW: return new PowFunction();
        case PI: return new PiFunction();
        case E: return new EFunction();
        case RANDOM: return new RandomFunction();
        case ABS: return new AbsFunction();
        default: return null;
        }
    }
    
    
    // SQRT
    class SqrtFunction extends TLFunctionPrototype {
        public SqrtFunction() {
            super("math", "sqrt", "Square root", new TLValueType[] { TLValueType.DECIMAL }, TLValueType.DOUBLE);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric()) {
                TLNumericValue retVal=(TLNumericValue)context.getContext();
                try {
                    retVal.setValue(Math.sqrt(((TLNumericValue)params[0]).getDouble()));
                    return retVal;
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing SQRT function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "sqrt - wrong type of literal(s)");
        }
        
        @Override
        public TLContext createContext() {
            return TLContext.createDoubleContext();
        }
    }
    
    // LOG
    class LogFunction extends TLFunctionPrototype {
        public LogFunction() {
            super("math", "log", "Natural logarithm", new TLValueType[] { TLValueType.DECIMAL }, TLValueType.DOUBLE);
        }
        
        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric()) {
                TLNumericValue retVal=(TLNumericValue)context.getContext();
                try {
                    retVal.setValue(Math.log(((TLNumericValue)params[0]).getDouble()));
                    return retVal;
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing LOG function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "log - wrong type of literal(s)");
        }
        @Override
        public TLContext createContext() {
            return TLContext.createDoubleContext();
        }
    }
    
    // LOG10
    class Log10Function extends TLFunctionPrototype {
        public Log10Function() {
            super("math", "log10", "Base 10 logarithm", new TLValueType[] { TLValueType.DECIMAL }, TLValueType.DOUBLE);
        }
        
        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric()) {
                TLNumericValue retVal=(TLNumericValue)context.getContext();
                try {
                    retVal.setValue(Math.log10(((TLNumericValue)params[0]).getDouble()));
                    return retVal;
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing LOG10 function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "log10 - wrong type of literal(s)");
        }
        @Override
        public TLContext createContext() {
            return TLContext.createDoubleContext();
        }
    }
 
    // EXP
    class ExpFunction extends TLFunctionPrototype {
        public ExpFunction() {
            super("math", "exp", "Returns exponent", new TLValueType[] { TLValueType.DECIMAL }, TLValueType.DOUBLE);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric()) {
                TLNumericValue retVal=(TLNumericValue)context.getContext();
                try {
                    retVal.setValue(Math.exp(((TLNumericValue)params[0])
                                    .getDouble()));
                    return retVal;
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing EXP function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "exp - wrong type of literal(s)");
        }
        @Override
        public TLContext createContext() {
            return TLContext.createDoubleContext();
        }
    }                        

    // ROUND
    class RoundFunction extends TLFunctionPrototype { 
        public RoundFunction() {
            super("math", "round", "Returns rounded value",  new TLValueType[] { TLValueType.DECIMAL }, TLValueType.DOUBLE);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric()) {
                TLNumericValue retVal=(TLNumericValue)context.getContext();
                try {
                   retVal.setValue(Math.round(((TLNumericValue)params[0])
                                    .getDouble()));
                    return retVal;
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing ROUND function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "round - wrong type of literal(s)");
        }
        @Override
        public TLContext createContext() {
            return TLContext.createDoubleContext();
        }
    }                        
    
    // POW
    class PowFunction extends TLFunctionPrototype { 
        public PowFunction() {
            super("math", "pow", "Calculates power", new TLValueType[] { TLValueType.DECIMAL, TLValueType.DECIMAL }, TLValueType.DOUBLE);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric()) {
                TLNumericValue retVal=(TLNumericValue)context.getContext();
                try {
                    retVal.setValue(Math.pow(((TLNumericValue)params[0]).getDouble(),((TLNumericValue)params[1]).getDouble()));
                    return retVal;
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing POW function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "pow - wrong type of literal(s)");
        }
        @Override
        public TLContext createContext() {
            return TLContext.createDoubleContext();
        }
    }                        
    
    // PI
    class PiFunction extends TLFunctionPrototype { 
        public PiFunction() {
            super("math", "pi", "The PI constant", new TLValueType[] { }, TLValueType.DOUBLE);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            return TLNumericValue.PI;
        }
    }         
    
    //  E
    class EFunction extends TLFunctionPrototype { 
        public EFunction() {
            super("math", "e", "The e constant", new TLValueType[] { }, TLValueType.DOUBLE);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            return TLNumericValue.E;
        }
    }     

    // RANDOM
    class RandomFunction extends TLFunctionPrototype {
        public RandomFunction() {
            super("math", "random", "Random number (>=0, <1)", new TLValueType[] { }, TLValueType.DOUBLE);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
                TLNumericValue retVal=(TLNumericValue)context.getContext();
                try {
                    retVal.setValue(Math.random());
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing RANDOM function", ex);
                }
                
                return retVal;
        }
        
        @Override
        public TLContext createContext() {
            return TLContext.createDoubleContext();
        }
    }
    
    // ABS
    class AbsFunction extends TLFunctionPrototype { 
        public AbsFunction() {
            super("math", "abs", "Absolute value", new TLValueType[] { TLValueType.DECIMAL }, TLValueType.DOUBLE);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric()) {
                TLNumericValue retVal=(TLNumericValue)context.getContext();
                try {
                   retVal.setValue(Math.abs(((TLNumericValue)params[0])
                                    .getDouble()));
                    return retVal;
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing ABS function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "abs - wrong type of literal(s)");
        }
        @Override
        public TLContext createContext() {
            return TLContext.createDoubleContext();
        }
    }                        


}
