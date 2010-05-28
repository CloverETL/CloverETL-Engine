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
package org.jetel.interpreter.extensions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.jetel.interpreter.TransformLangExecutorRuntimeException;
import org.jetel.interpreter.data.TLBooleanValue;
import org.jetel.interpreter.data.TLNumericValue;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.util.DataGenerator;

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
        RANDOM_GAUSSIAN("random_gaussian"),
        RANDOM_BOOLEAN("random_boolean"),
        RANDOM_INT("random_int"),
        RANDOM_LONG("random_long"),
        ABS("abs"),
        BIT_AND("bit_and"),
        BIT_OR("bit_or"),
        BIT_XOR("bit_xor"),
        BIT_SET("bit_set"),
        BIT_IS_SET("bit_is_set"),
        BIT_LSHIFT("bit_lshift"),
        BIT_RSHIFT("bit_rshift"),
        BIT_INVERT("bit_invert");
        
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
        case RANDOM_GAUSSIAN: return new RandomGaussianFunction();
        case RANDOM_BOOLEAN: return new RandomBooleanFunction();
        case RANDOM_INT: return new RandomIntFunction();
        case RANDOM_LONG: return new RandomLongFunction();
        case ABS: return new AbsFunction();
        case BIT_AND: return new BitAndFunction();
        case BIT_OR: return new BitOrFunction();
        case BIT_XOR: return new BitXorFunction();
        case BIT_SET: return new BitSetFunction();
        case BIT_IS_SET: return new BitIsSetFunction();
        case BIT_LSHIFT: return new BitLShiftFunction();
        case BIT_RSHIFT: return new BitRShiftFunction();
        case BIT_INVERT: return new BitInvertFunction();
        default: return null;
        }
    }
    
    
    // SQRT
    class SqrtFunction extends TLFunctionPrototype {
        public SqrtFunction() {
            super("math", "sqrt", "Square root", new TLValueType[] { TLValueType.DECIMAL }, TLValueType.NUMBER);
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
            super("math", "log", "Natural logarithm", new TLValueType[] { TLValueType.DECIMAL }, TLValueType.NUMBER);
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
            super("math", "log10", "Base 10 logarithm", new TLValueType[] { TLValueType.DECIMAL }, TLValueType.NUMBER);
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
            super("math", "exp", "Returns exponent", new TLValueType[] { TLValueType.DECIMAL }, TLValueType.NUMBER);
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
            super("math", "round", "Returns rounded value",  new TLValueType[] { TLValueType.DECIMAL }, TLValueType.NUMBER);
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
            super("math", "pow", "Calculates power", new TLValueType[] { TLValueType.DECIMAL, TLValueType.DECIMAL }, TLValueType.NUMBER);
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
            super("math", "pi", "The PI constant", new TLValueType[] { }, TLValueType.NUMBER);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            return TLNumericValue.PI;
        }
    }         
    
    //  E
    class EFunction extends TLFunctionPrototype { 
        public EFunction() {
            super("math", "e", "The e constant", new TLValueType[] { }, TLValueType.NUMBER);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            return TLNumericValue.E;
        }
    }     

    // RANDOM
    class RandomFunction extends TLFunctionPrototype {
    	
        public RandomFunction() {
            super("math", "random", "Random number (>=0, <1)", new TLValueType[] { TLValueType.LONG }, TLValueType.NUMBER, 1, 0);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
        	RandomContext retVal=(RandomContext)context.getContext();
        	DataGenerator dataGenerator = retVal.getDataGenerator();

        	if (params.length == 1) {
            	if (params[0].type != TLValueType.LONG && params[0].type != TLValueType.INTEGER) {
    	            throw new TransformLangExecutorRuntimeException(params, "random - wrong type of the third literal");
            	}
        		if (retVal.randomSeed != params[0].getNumeric().getLong()) {
        			retVal.randomSeed = params[0].getNumeric().getLong();
        			dataGenerator.setSeed(retVal.randomSeed);
        		}
        	}
            try {
                retVal.setValue(dataGenerator.nextDouble());
            } catch (Exception ex) {
                throw new TransformLangExecutorRuntimeException(
                        "Error when executing RANDOM function", ex);
            }
            
            return retVal.value;
        }
        
        @Override
        public TLContext createContext() {
            return RandomContext.createDoubleContext();
        }
    }
    
    // RANDOM Gaussian
    class RandomGaussianFunction extends TLFunctionPrototype {

    	public RandomGaussianFunction() {
            super("math", "random_gaussian", "Random Gaussian number", new TLValueType[] { TLValueType.LONG }, TLValueType.NUMBER, 1, 0);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
        	RandomContext retVal=(RandomContext)context.getContext();
        	DataGenerator dataGenerator = retVal.getDataGenerator();
        	
        	if (params.length == 1) {
            	if (params[0].type != TLValueType.LONG && params[0].type != TLValueType.INTEGER) {
    	            throw new TransformLangExecutorRuntimeException(params, "random_gaussian - wrong type of the third literal");
            	}
        		if (retVal.randomSeed != params[0].getNumeric().getLong()) {
        			retVal.randomSeed = params[0].getNumeric().getLong();
        			dataGenerator.setSeed(retVal.randomSeed);
        		}
        	}
            try {
                retVal.setValue(dataGenerator.nextGaussian());
            } catch (Exception ex) {
                throw new TransformLangExecutorRuntimeException(
                        "Error when executing RANDOM Gaussian function", ex);
            }
            
            return retVal.value;
        }
        
        @Override
        public TLContext createContext() {
            return RandomContext.createDoubleContext();
        }
    }

    // RANDOM Boolean
    class RandomBooleanFunction extends TLFunctionPrototype {

    	public RandomBooleanFunction() {
            super("math", "random_boolean", "Random boolean", new TLValueType[] { TLValueType.LONG }, TLValueType.BOOLEAN, 1, 0);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
        	RandomContext retVal=(RandomContext)context.getContext();
        	DataGenerator dataGenerator = retVal.getDataGenerator();

        	if (params.length == 1) {
            	if (params[0].type != TLValueType.LONG && params[0].type != TLValueType.INTEGER) {
    	            throw new TransformLangExecutorRuntimeException(params, "random_boolean - wrong type of the third literal");
            	}
        		if (retVal.randomSeed != params[0].getNumeric().getLong()) {
        			retVal.randomSeed = params[0].getNumeric().getLong();
        			dataGenerator.setSeed(retVal.randomSeed);
        		}
        	}
        	return dataGenerator.nextBoolean() ? TLBooleanValue.TRUE : TLBooleanValue.FALSE;
        }
        
        @Override
        public TLContext createContext() {
            return RandomContext.createBooleanContext();
        }
    }
    
    // RANDOM_INT
    class RandomIntFunction extends TLFunctionPrototype {
    	
        public RandomIntFunction() {
            super("math", "random_int", "Random integer", 
            		new TLValueType[] { TLValueType.LONG, TLValueType.INTEGER, TLValueType.LONG }, TLValueType.INTEGER, 3, 0);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
        	RandomContext val = (RandomContext)context.getContext();
        	DataGenerator dataGenerator = val.getDataGenerator();
            
            // random_int()
            if (params.length == 0) {
                try {
                	val.setValue(dataGenerator.nextInt());
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException("Error when executing RANDOM_INT function", ex);
                }
            
            // random_int(min, max)
            } else if (params.length >= 2) {
            	if (params[0].type != TLValueType.INTEGER) {
    	            throw new TransformLangExecutorRuntimeException(params, "random_int - wrong type of the first literal");
            	}
            	if (params[1].type != TLValueType.INTEGER) {
    	            throw new TransformLangExecutorRuntimeException(params, "random_int - wrong type of the second literal");
            	}
            	if (params.length == 3) {
                	if (params[2].type != TLValueType.LONG && params[2].type != TLValueType.INTEGER) {
        	            throw new TransformLangExecutorRuntimeException(params, "random_int - wrong type of the third literal");
                	}
            		if (val.randomSeed != params[2].getNumeric().getLong()) {
            			val.randomSeed = params[2].getNumeric().getLong();
            			dataGenerator.setSeed(val.randomSeed);
            		}
            	}
                try {
                	val.setValue(dataGenerator.nextInt(params[0].getNumeric().getInt(), params[1].getNumeric().getInt()));
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing RANDOM_INT function", ex);
                }            	
                
            // random_int(randomSeed)
            } else if (params.length == 1) {
            	if (params[0].type != TLValueType.LONG && params[0].type != TLValueType.INTEGER) {
    	            throw new TransformLangExecutorRuntimeException(params, "random_int - wrong type of the third literal");
            	}
        		if (val.randomSeed != params[0].getNumeric().getLong()) {
        			val.randomSeed = params[0].getNumeric().getLong();
        			dataGenerator.setSeed(val.randomSeed);
        		}
            	
                try {
                	val.setValue(dataGenerator.nextInt());
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing RANDOM_INT function", ex);
                }            	

            // wrong count of parameters (one parameter)
            } else {
	            throw new TransformLangExecutorRuntimeException(params, "random_int - wrong count of parameters");
            }
                
            return val.value;
        }
        
        @Override
        public TLContext createContext() {
            return RandomContext.createIntegerContext();
        }
    }

    // RANDOM_LONG
    class RandomLongFunction extends TLFunctionPrototype {
    	
        public RandomLongFunction() {
            super("math", "random_long", "Random long", 
            		new TLValueType[] { TLValueType.LONG, TLValueType.LONG, TLValueType.LONG }, TLValueType.LONG, 3, 0);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
        	RandomContext val = (RandomContext)context.getContext();
        	DataGenerator dataGenerator = val.getDataGenerator();

            // random_long()
            if (params.length == 0) {
                try {
                	val.setValue(dataGenerator.nextLong());
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException("Error when executing RANDOM_LONG function", ex);
                }
            
            // random_long(min, max)
            } else if (params.length >= 2) {
            	if (!(params[0].type == TLValueType.LONG || params[0].type == TLValueType.INTEGER)) {
    	            throw new TransformLangExecutorRuntimeException(params, "random_long - wrong type of the first literal");
            	}
            	if (!(params[1].type == TLValueType.LONG || params[1].type == TLValueType.INTEGER)) {
    	            throw new TransformLangExecutorRuntimeException(params, "random_long - wrong type of the second literal");
            	}
            	if (params.length == 3) {
                	if (params[2].type != TLValueType.LONG && params[2].type != TLValueType.INTEGER) {
        	            throw new TransformLangExecutorRuntimeException(params, "random_long - wrong type of the third literal");
                	}
            		if (val.randomSeed != params[2].getNumeric().getLong()) {
            			val.randomSeed = params[2].getNumeric().getLong();
            			dataGenerator.setSeed(val.randomSeed);
            		}
            	}
                try {
                	val.setValue(dataGenerator.nextLong(params[0].getNumeric().getLong(), params[1].getNumeric().getLong()));
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing RANDOM_LONG function", ex);
                }            	

            // random_long(randomSeed)
            } else if (params.length == 1) {
            	if (params[0].type != TLValueType.LONG && params[0].type != TLValueType.INTEGER) {
    	            throw new TransformLangExecutorRuntimeException(params, "random_long - wrong type of the third literal");
            	}
        		if (val.randomSeed != params[0].getNumeric().getLong()) {
        			val.randomSeed = params[0].getNumeric().getLong();
        			dataGenerator.setSeed(val.randomSeed);
        		}
                
                try {
                	val.setValue(dataGenerator.nextLong());
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing RANDOM_LONG function", ex);
                }            	

            // wrong count of parameters (one parameter)
            } else {
	            throw new TransformLangExecutorRuntimeException(params, "random_long - wrong count of parameters");
            }
                
            return val.value;
        }
        
        @Override
        public TLContext createContext() {
            return RandomContext.createLongContext();
        }
    }

    // ABS
    class AbsFunction extends TLFunctionPrototype { 
        public AbsFunction() {
            super("math", "abs", "Absolute value", new TLValueType[] { TLValueType.DECIMAL }, TLValueType.NUMBER);
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

    // BIT_AND
    class BitAndFunction extends TLFunctionPrototype { 
        public BitAndFunction() {
            super("math", "bit_and", "Bit wise AND on parameters ", new TLValueType[] { TLValueType.NUMBER, TLValueType.NUMBER}, TLValueType.LONG);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric() && params[1].type.isNumeric()) {
                TLNumericValue retVal=(TLNumericValue)context.getContext();
                try {
                   retVal.setValue(((TLNumericValue)params[0]).getLong() & ((TLNumericValue)params[1]).getLong());
                    return retVal;
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing BIT_AND function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "bit_and - wrong type of literal(s)");
        }
        @Override
        public TLContext createContext() {
            return TLContext.createLongContext();
        }
    }                     

 // BIT_OR
    class BitOrFunction extends TLFunctionPrototype { 
        public BitOrFunction() {
            super("math", "bit_or", "Bit wise OR on parameters ", new TLValueType[] { TLValueType.NUMBER, TLValueType.NUMBER}, TLValueType.LONG);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric() && params[1].type.isNumeric()) {
                TLNumericValue retVal=(TLNumericValue)context.getContext();
                try {
                   retVal.setValue(((TLNumericValue)params[0]).getLong() | ((TLNumericValue)params[1]).getLong());
                    return retVal;
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing BIT_OR function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "bit_or - wrong type of literal(s)");
        }
        @Override
        public TLContext createContext() {
            return TLContext.createLongContext();
        }
    }              
    
 // BIT_XOR
    class BitXorFunction extends TLFunctionPrototype { 
        public BitXorFunction() {
            super("math", "bit_xor", "Bit wise AND on parameters ", new TLValueType[] { TLValueType.NUMBER, TLValueType.NUMBER}, TLValueType.LONG);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric() && params[1].type.isNumeric()) {
                TLNumericValue retVal=(TLNumericValue)context.getContext();
                try {
                   retVal.setValue(((TLNumericValue)params[0]).getLong() ^ ((TLNumericValue)params[1]).getLong());
                    return retVal;
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing BIT_XOR function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "bit_xor - wrong type of literal(s)");
        }
        @Override
        public TLContext createContext() {
            return TLContext.createLongContext();
        }
    }              
    
 // BIT_IS_SET
    class BitIsSetFunction extends TLFunctionPrototype { 
        public BitIsSetFunction() {
            super("math", "bit_is_set", "Tests if n-th bit of 1st argument is set", new TLValueType[] { TLValueType.NUMBER, TLValueType.NUMBER}, TLValueType.BOOLEAN);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric() && params[1].type.isNumeric()) {               
                try {
                	if ((((TLNumericValue)params[0]).getLong() & ( 1 << ((TLNumericValue)params[1]).getLong())) != 0)
                		return TLBooleanValue.TRUE;
                	else
                		return TLBooleanValue.FALSE;
                	
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing BIT_IS_SET function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "bit_is_set - wrong type of literal(s)");
        }
    }           
    
 // BIT_SET
    class BitSetFunction extends TLFunctionPrototype { 
        public BitSetFunction() {
            super("math", "bit_set", "Sets or resets n-th bit of 1st argument",
            		new TLValueType[] { TLValueType.NUMBER, TLValueType.NUMBER, TLValueType.BOOLEAN}, TLValueType.NUMBER);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric() && params[1].type.isNumeric() && params[2].type==TLValueType.BOOLEAN) {               
                try {
                	TLNumericValue retVal=(TLNumericValue)context.getContext();
                	 
                	if (((TLBooleanValue)params[2])==TLBooleanValue.TRUE){
                		retVal.setValue((((TLNumericValue)params[0]).getLong() | ( 1 << ((TLNumericValue)params[1]).getLong()) )); 
                	}else{
                		retVal.setValue((((TLNumericValue)params[0]).getLong() & (~( 1 << ((TLNumericValue)params[1]).getLong()))));
                	}
                	
                	return retVal;
                
                	
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing BIT_SET function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "bit_set - wrong type of literal(s)");
        }
        @Override
        public TLContext createContext() {
            return TLContext.createLongContext();
        }
    }         
    
 // BIT_LSHIFT
    class BitLShiftFunction extends TLFunctionPrototype { 
        public BitLShiftFunction() {
            super("math", "bit_lshift", "Shifts 1st argument by 2nd argument bits to the left", new TLValueType[] { TLValueType.NUMBER, TLValueType.NUMBER}, TLValueType.NUMBER);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric() && params[1].type.isNumeric() ) {               
                try {
                	TLNumericValue retVal=(TLNumericValue)context.getContext();
                	 
               		retVal.setValue(((TLNumericValue)params[0]).getLong() << ((TLNumericValue)params[1]).getLong()); 
                	return retVal;
                
                	
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing BIT_LSHIFT function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "bit_lshift - wrong type of literal(s)");
        }
        @Override
        public TLContext createContext() {
            return TLContext.createLongContext();
        }
    }       
    
 // BIT_RSHIFT
    class BitRShiftFunction extends TLFunctionPrototype { 
        public BitRShiftFunction() {
            super("math", "bit_rshift", "Shifts 1st argument by 2nd argument bits to the right", new TLValueType[] { TLValueType.NUMBER, TLValueType.NUMBER}, TLValueType.NUMBER);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric() && params[1].type.isNumeric() ) {               
                try {
                	TLNumericValue retVal=(TLNumericValue)context.getContext();
                	 
               		retVal.setValue(((TLNumericValue)params[0]).getLong() >> ((TLNumericValue)params[1]).getLong()); 
                	return retVal;
                
                	
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing BIT_RSHIFT function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "bit_rshift - wrong type of literal(s)");
        }
        @Override
        public TLContext createContext() {
            return TLContext.createLongContext();
        }
    }    
    
 // BIT_INVERT
    class BitInvertFunction extends TLFunctionPrototype { 
        public BitInvertFunction() {
            super("math", "bit_invert", "Inverts all bits in argument", new TLValueType[] { TLValueType.NUMBER}, TLValueType.NUMBER);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            if (params[0].type.isNumeric() ) {               
                try {
                	TLNumericValue retVal=(TLNumericValue)context.getContext();
                	 
               		retVal.setValue(~((TLNumericValue)params[0]).getLong()); 
                	return retVal;
                
                	
                } catch (Exception ex) {
                    throw new TransformLangExecutorRuntimeException(
                            "Error when executing BIT_INVERT function", ex);
                }
            }
            throw new TransformLangExecutorRuntimeException(null,
                    params, "bit_invert - wrong type of literal(s)");
        }
        @Override
        public TLContext createContext() {
            return TLContext.createLongContext();
        }
    }    
    
    /**
     * Context for random values. 
     */
	private static class RandomContext {
		
    	private DataGenerator dataGenerator = new DataGenerator();
		
		// random seed
		private long randomSeed = Long.MIN_VALUE;

		// random value
		private TLValue value;
		
		// set value		
		public void setValue(int nextInt) {
			value.setValue(Integer.valueOf(nextInt));
		}
		
		public void setValue(double nextDouble) {
			value.setValue(Double.valueOf(nextDouble));
		}

		public void setValue(long nextLong) {
			value.setValue(Long.valueOf(nextLong));
		}

		public DataGenerator getDataGenerator() {
			return dataGenerator;
		}
		
	    public static TLContext createDoubleContext() {
	    	RandomContext con = new RandomContext();
	    	con.value = TLValue.create(TLValueType.NUMBER);
	    	
	        TLContext<RandomContext> context=new TLContext<RandomContext>();
	        context.setContext(con);
	        return context;        	
		}

		public static TLContext createLongContext() {
	    	RandomContext con = new RandomContext();
	    	con.value = TLValue.create(TLValueType.LONG);
	    	
	        TLContext<RandomContext> context=new TLContext<RandomContext>();
	        context.setContext(con);
	        return context;        	
		}

		public static TLContext<RandomContext> createIntegerContext(){
	    	RandomContext con = new RandomContext();
	    	con.value = TLValue.create(TLValueType.INTEGER);
	    	
	        TLContext<RandomContext> context=new TLContext<RandomContext>();
	        context.setContext(con);
	        return context;        	
	    }

		public static TLContext<RandomContext> createBooleanContext() {
	    	RandomContext con = new RandomContext();
	    	con.value = TLValue.create(TLValueType.BOOLEAN);
	    	
	        TLContext<RandomContext> context=new TLContext<RandomContext>();
	        context.setContext(con);
	        return context;
		}
	}
	
}
