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

import org.jetel.data.primitive.CloverDouble;
import org.jetel.interpreter.TransformLangExecutorRuntimeException;
import org.jetel.interpreter.data.TLContext;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;

public class MathLib implements ITLFunctionLibrary {
    
    private static final String LIBRARY_NAME = "Math";
    
    enum Function {
        SQRT("sqrt"),
        LOG("log"),
        LOG10("log10"),
        EXP("exp"),
        ROUND("round"),
        POW("pow"),
        PI("pi"),
        E("E"),
        RANDOM("random");
        
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
        default: return null;
        }
    }
    
    // SQRT
    class SqrtFunction extends TLFunctionPrototype {
        public SqrtFunction() {
            super("math", "sqrt", new TLValueType[] { TLValueType.DECIMAL }, TLValueType.DOUBLE);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric()) {
                TLValue retVal;
                try {
                    retVal = new TLValue(TLValueType.DOUBLE,
                            new CloverDouble(Math.sqrt(params[0]
                                    .getDouble())));
                    return retVal;
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing SQRT function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "sqrt - wrong type of literal(s)");
        }
    }
    
    // LOG
    class LogFunction extends TLFunctionPrototype {
        public LogFunction() {
            super("math", "log", new TLValueType[] { TLValueType.DECIMAL }, TLValueType.DOUBLE);
        }
        
        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric()) {
                TLValue retVal;
                try {
                    retVal = new TLValue(TLValueType.DOUBLE,
                            new CloverDouble(Math.log((params[0]
                                    .getDouble()))));
                    return retVal;
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing LOG function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "log - wrong type of literal(s)");
        }
    }
    
    // LOG10
    class Log10Function extends TLFunctionPrototype {
        public Log10Function() {
            super("math", "log10", new TLValueType[] { TLValueType.DECIMAL }, TLValueType.DOUBLE);
        }
        
        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric()) {
                TLValue retVal;
                try {
                    retVal = new TLValue(TLValueType.DOUBLE,
                            new CloverDouble(Math.log10((params[0]
                                    .getDouble()))));
                    return retVal;
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing LOG10 function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "log10 - wrong type of literal(s)");
        }
    }
 
    // EXP
    class ExpFunction extends TLFunctionPrototype {
        public ExpFunction() {
            super("math", "exp", new TLValueType[] { TLValueType.DECIMAL }, TLValueType.DOUBLE);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric()) {
                TLValue retVal;
                try {
                    retVal = new TLValue(TLValueType.DOUBLE,
                            new CloverDouble(Math.exp((params[0]
                                    .getDouble()))));
                    return retVal;
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing EXP function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "exp - wrong type of literal(s)");
        }
    }                        

    // ROUND
    class RoundFunction extends TLFunctionPrototype { 
        public RoundFunction() {
            super("math", "round", new TLValueType[] { TLValueType.DECIMAL }, TLValueType.DOUBLE);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric()) {
                TLValue retVal;
                try {
                    retVal = new TLValue(TLValueType.DOUBLE,
                            new CloverDouble(Math.round((params[0]
                                    .getDouble()))));
                    return retVal;
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing ROUND function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "round - wrong type of literal(s)");
        }
    }                        
    
    // POW
    class PowFunction extends TLFunctionPrototype { 
        public PowFunction() {
            super("math", "pow", new TLValueType[] { TLValueType.DECIMAL, TLValueType.DECIMAL }, TLValueType.DOUBLE);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric()) {
                TLValue retVal;
                try {
                    retVal = new TLValue(TLValueType.DOUBLE,
                            new CloverDouble(Math.pow(params[0].getDouble(),params[1].getDouble())));
                    return retVal;
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing POW function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "pow - wrong type of literal(s)");
        }
    }                        
    
    // PI
    class PiFunction extends TLFunctionPrototype { 
        public PiFunction() {
            super("math", "pi", new TLValueType[] { }, TLValueType.DOUBLE);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            return TLValue.NUM_PI_VAL;
        }
    }         
    
    //  E
    class EFunction extends TLFunctionPrototype { 
        public EFunction() {
            super("math", "E", new TLValueType[] { }, TLValueType.DOUBLE);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            return TLValue.NUM_E_VAL;
        }
    }     

    // RANDOM
    class RandomFunction extends TLFunctionPrototype {
        public RandomFunction() {
            super("math", "random", new TLValueType[] { }, TLValueType.DOUBLE);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric()) {
                TLValue retVal;
                try {
                    retVal = new TLValue(TLValueType.DOUBLE,
                            new CloverDouble(Math.random()));
                    return retVal;
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing RANDOM function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "random - wrong type of literal(s)");
        }
    }                        

}
