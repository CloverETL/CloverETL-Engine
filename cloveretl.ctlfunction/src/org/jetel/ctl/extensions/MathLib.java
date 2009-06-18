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
package org.jetel.ctl.extensions;

import java.math.BigDecimal;

import org.jetel.ctl.Stack;
import org.jetel.ctl.data.TLType;
import org.jetel.ctl.extensions.TLFunctionLibrary;
import org.jetel.ctl.extensions.TLFunctionPrototype;
import org.jetel.ctl.extensions.StringLib.CharAtFunction;
import org.jetel.ctl.extensions.StringLib.ChopFunction;
import org.jetel.ctl.extensions.StringLib.ConcatFunction;
import org.jetel.ctl.extensions.StringLib.CountCharFunction;
import org.jetel.ctl.extensions.StringLib.CutFunction;
import org.jetel.ctl.extensions.StringLib.FindFunction;
import org.jetel.ctl.extensions.StringLib.GetAlphanumericCharsFunction;
import org.jetel.ctl.extensions.StringLib.IndexOfFunction;
import org.jetel.ctl.extensions.StringLib.IsAsciiFunction;
import org.jetel.ctl.extensions.StringLib.IsBlankFunction;
import org.jetel.ctl.extensions.StringLib.IsDateFunction;
import org.jetel.ctl.extensions.StringLib.IsIntegerFunction;
import org.jetel.ctl.extensions.StringLib.IsLongFunction;
import org.jetel.ctl.extensions.StringLib.IsNumberFunction;
import org.jetel.ctl.extensions.StringLib.JoinFunction;
import org.jetel.ctl.extensions.StringLib.LeftFunction;
import org.jetel.ctl.extensions.StringLib.LengthFunction;
import org.jetel.ctl.extensions.StringLib.LowerCaseFunction;
import org.jetel.ctl.extensions.StringLib.RemoveBlankSpaceFunction;
import org.jetel.ctl.extensions.StringLib.RemoveDiacriticFunction;
import org.jetel.ctl.extensions.StringLib.RemoveNonAsciiFunction;
import org.jetel.ctl.extensions.StringLib.RemoveNonPrintableFunction;
import org.jetel.ctl.extensions.StringLib.ReplaceFunction;
import org.jetel.ctl.extensions.StringLib.RightFunction;
import org.jetel.ctl.extensions.StringLib.SoundexFunction;
import org.jetel.ctl.extensions.StringLib.SplitFunction;
import org.jetel.ctl.extensions.StringLib.SubstringFunction;
import org.jetel.ctl.extensions.StringLib.TranslateFunction;
import org.jetel.ctl.extensions.StringLib.TrimFunction;
import org.jetel.ctl.extensions.StringLib.UpperCaseFunction;


public class MathLib extends TLFunctionLibrary {
    
	@Override
	public TLFunctionPrototype getExecutable(String functionName) {
		TLFunctionPrototype ret = 
			"sqrt".equals(functionName) ? new SqrtFunction() :
			"log".equals(functionName) ? new LogFunction() :
			"log10".equals(functionName) ? new Log10Function() :
			"round".equals(functionName) ? new RoundFunction() :
			"pow".equals(functionName) ? new PowFunction() : 
			null;
			
		if (ret == null) {
    		throw new IllegalArgumentException("Unknown function '" + functionName + "'");
    	}

		return ret;
	}
	
    
	@TLFunctionAnnotation("Square root.")
	public static final Double sqrt(double i) {
		return Math.sqrt(i);
	}
	
	@TLFunctionAnnotation("Square root. Decimal is converted into double prior to the operation.")
	public static final Double sqrt(BigDecimal b) {
		return sqrt(b.doubleValue());
	}
	
    // SQRT
    class SqrtFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			
			if (actualParams[0].isInteger()) {
				stack.push(sqrt(stack.popInt()));
				return;
			} 
			
			if (actualParams[0].isLong()) {
				stack.push(sqrt(stack.popLong()));
				return;
			} 
			
			if (actualParams[0].isDouble()) {
				stack.push(sqrt(stack.popDouble()));
				return;
			} 
			
			if (actualParams[0].isDecimal()) {
				stack.push(sqrt(stack.popDecimal()));
				return;
			}
			
		}
    	
    }

    
    @TLFunctionAnnotation("Natural algorithm.")
    public static final Double log(double d) {
    	return Math.log(d);
    }
    
    @TLFunctionAnnotation("Natural algorithm. Decimal is converted into double prior to the operation.")
    public static final Double log(BigDecimal d) {
    	return log(d.doubleValue());
    }
    	
    // LOG
    class LogFunction implements TLFunctionPrototype {
    	public void execute(Stack stack, TLType[] actualParams) {
			
			if (actualParams[0].isInteger()) {
				stack.push(log(stack.popInt()));
				return;
			} 
			
			if (actualParams[0].isLong()) {
				stack.push(log(stack.popLong()));
				return;
			} 
			
			if (actualParams[0].isDouble()) {
				stack.push(log(stack.popDouble()));
				return;
			} 
			
			if (actualParams[0].isDecimal()) {
				stack.push(log(stack.popDecimal()));
				return;
			}
			
		}
    }
    
    
    @TLFunctionAnnotation("Base 10 logarithm.")
    public static final Double log10(double d) {
    	return Math.log10(d);
    }
    
    @TLFunctionAnnotation("Base 10 logarithm. Decimal is converted into double prior to the operation.")
    public static final Double log10(BigDecimal d) {
    	return log10(d.doubleValue());
    }
    	
    // LOG
    class Log10Function implements TLFunctionPrototype {
    	public void execute(Stack stack, TLType[] actualParams) {
			
			if (actualParams[0].isInteger()) {
				stack.push(log10(stack.popInt()));
				return;
			} 
			
			if (actualParams[0].isLong()) {
				stack.push(log10(stack.popLong()));
				return;
			} 
			
			if (actualParams[0].isDouble()) {
				stack.push(log10(stack.popDouble()));
				return;
			} 
			
			if (actualParams[0].isDecimal()) {
				stack.push(log10(stack.popDecimal()));
				return;
			}
			
		}
    }
    

    @TLFunctionAnnotation("Returns long value closest to the argument.")
    public static final Long round(double d) {
    	return Math.round(d);
    }
    
    @TLFunctionAnnotation("Returns long value closest to the argument. Decimal is converted into double prior to the operation.")
    public static final Long round(BigDecimal d) {
    	return round(d.doubleValue()); 
    }
    
    // ROUND
    class RoundFunction implements TLFunctionPrototype { 
    	public void execute(Stack stack, TLType[] actualParams) {
			
			if (actualParams[0].isDecimal()) {
				stack.push(round(stack.popDecimal()));
				return;
			}
			
			stack.push(round(stack.popDouble()));
		}
    }                        
    
    @TLFunctionAnnotation("Returns the value of the first argument raised to the power of the second argument.")
    public static final Double pow(double argument, double power) {
    	return Math.pow(argument, power);
    }
    
    @TLFunctionAnnotation("Returns the value of the first argument raised to the power of the second argument.")
    public static final Double pow(BigDecimal argument, BigDecimal power) {
    	return Math.pow(argument.doubleValue(), power.doubleValue());
    }
    
    // POW
    class PowFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			if (actualParams[0].isDecimal() || actualParams[1].isDecimal()) {
				final BigDecimal pow = stack.popDecimal();
				final BigDecimal arg = stack.popDecimal();
				stack.push(pow(arg,pow));
				return;
			}
			
			final Double pow = stack.popDouble();
			final Double arg = stack.popDouble();
			stack.push(pow(arg,pow));
		} 
    	
    }                        
    
//    // PI
//    class PiFunction extends TLFunctionPrototype { 
//        public PiFunction() {
//            super("math", "pi", "The PI constant", 
//            		new TLType[][] { }, TLTypePrimitive.DOUBLE);
//        }
//
//        @Override
//        public TLValue execute(TLValue[] params, TLContext context) {
//            return TLNumericValue.PI;
//        }
//        
////        @Override
////        public TLType checkParameters(TLType[] parameters) {
////        	if (parameters.length > 0) {
////        		return TLType.ERROR;
////        	}
////        	
////        	return TLTypePrimitive.DOUBLE;
////        }
//    }         
//    
//    //  E
//    class EFunction extends TLFunctionPrototype { 
//        public EFunction() {
//            super("math", "e", "The e constant", new TLType[][] {  }, TLTypePrimitive.DOUBLE);
//        }
//
//        @Override
//        public TLValue execute(TLValue[] params, TLContext context) {
//            return TLNumericValue.E;
//        }
//        
////        @Override
////        public TLType checkParameters(TLType[] parameters) {
////        	if (parameters.length > 0) {
////        		return TLType.ERROR;
////        	}
////        	
////        	return TLTypePrimitive.DOUBLE;
////        }
//    }     
//
//    // RANDOM
//    class RandomFunction extends TLFunctionPrototype {
//        public RandomFunction() {
//            super("math", "random", "Random number (>=0, <1)", 
//            		new TLType[][] { }, TLTypePrimitive.DOUBLE);
//        }
//
//        @Override
//        public TLValue execute(TLValue[] params, TLContext context) {
//                TLNumericValue retVal=(TLNumericValue)context.getContext();
//                try {
//                    retVal.setValue(Math.random());
//                } catch (Exception ex) {
//                    throw new TransformLangExecutorRuntimeException(
//                            "Error when executing RANDOM function", ex);
//                }
//                
//                return retVal;
//        }
//        
//        @Override
//        public TLContext createContext() {
//            return TLContext.createDoubleContext();
//        }
//        
////        @Override
////        public TLType checkParameters(TLType[] parameters) {
////        	if (parameters.length > 0) {
////        		return TLType.ERROR;
////        	}
////        	
////        	return TLTypePrimitive.DOUBLE;
////        }
//    }
//    
//    // ABS
//    class AbsFunction extends TLFunctionPrototype { 
//        public AbsFunction() {
//            super("math", "abs", "Absolute value", 
//            		new TLType[][] {
//            		{ TLTypePrimitive.INTEGER, TLTypePrimitive.LONG, 
//            			TLTypePrimitive.DOUBLE, TLTypePrimitive.DECIMAL}  }, 
//            			TLTypePrimitive.DOUBLE);
//        }
//
//        @Override
//        public TLValue execute(TLValue[] params, TLContext context) {
//            if (params[0].type.isNumeric()) {
//                TLNumericValue retVal=(TLNumericValue)context.getContext();
//                try {
//                   retVal.setValue(Math.abs(((TLNumericValue)params[0])
//                                    .getDouble()));
//                    return retVal;
//                } catch (Exception ex) {
//                    throw new TransformLangExecutorRuntimeException(
//                            "Error when executing ABS function", ex);
//                }
//            }
//            throw new TransformLangExecutorRuntimeException(null,
//                    params, "abs - wrong type of literal(s)");
//        }
//        @Override
//        public TLContext createContext() {
//            return TLContext.createDoubleContext();
//        }
//        
////        @Override
////        public TLType checkParameters(TLType[] parameters) {
////        	if (parameters.length != 1) {
////        		return TLType.ERROR;
////        	}
////        	
////        	if (! parameters[0].isNumeric()) {
////        		return TLType.ERROR;
////        	}
////        	
////        	return TLTypePrimitive.DOUBLE;
////        }
//    }                        


}
