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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Iterator;
import java.util.List;

import org.jetel.ctl.Stack;

public class MathLib extends TLFunctionLibrary {
	
	@Override
	public TLFunctionPrototype getExecutable(String functionName) {
		TLFunctionPrototype ret = 
			"sqrt".equals(functionName) ? new SqrtFunction() :
			"log".equals(functionName) ? new LogFunction() :
			"log10".equals(functionName) ? new Log10Function() :
			"exp".equals(functionName) ? new ExpFunction() :
			"floor".equals(functionName) ? new FloorFunction() :
			"round".equals(functionName) ? new RoundFunction() :
			"ceil".equals(functionName) ? new CeilFunction() :
			"pow".equals(functionName) ? new PowFunction() :
			"pi".equals(functionName) ? new PiFunction() :
			"e".equals(functionName) ? new EFunction() :
			"abs".equals(functionName) ? new AbsFunction() :
			"bitOr".equals(functionName) ? new BitOrFunction() :
			"bitAnd".equals(functionName) ? new BitAndFunction() :
			"bitXor".equals(functionName) ? new BitXorFunction() :
			"bitLShift".equals(functionName) ? new BitLShiftFunction() :
			"bitRShift".equals(functionName) ? new BitRShiftFunction() :
			"bitNegate".equals(functionName) ? new BitNegateFunction() :
			"bitSet".equals(functionName) ? new BitSetFunction() :
			"bitIsSet".equals(functionName) ? new BitIsSetFunction() :
			"cos".equals(functionName) ? new CosFunction():
			"sin".equals(functionName) ? new SinFunction():
			"tan".equals(functionName) ? new TanFunction():
			"acos".equals(functionName) ? new ACosFunction():
			"asin".equals(functionName) ? new ASinFunction():
			"atan".equals(functionName) ? new ATanFunction():
			"min".equals(functionName) ? new MinFunction():
			"max".equals(functionName) ? new MaxFunction():
			"signum".equals(functionName) ? new SignumFunction():
			"toDegrees".equals(functionName) ? new ToDegreesFunction():
			"toRadians".equals(functionName) ? new ToRadiansFunction():
				
			null;
			
		if (ret == null) {
    		throw new IllegalArgumentException("Unknown function '" + functionName + "'");
    	}

		return ret;
	}
	
	private static String LIBRARY_NAME = "Math";

	@Override
	public String getName() {
		return LIBRARY_NAME;
	}

	
    
	@TLFunctionAnnotation("Square root.")
	public static final Double sqrt(TLFunctionCallContext context, double i) {
		return Math.sqrt(i);
	}
	
	@TLFunctionAnnotation("Square root. Decimal is converted to double prior to the operation.")
	public static final Double sqrt(TLFunctionCallContext context, BigDecimal b) {
		return sqrt(context, b.doubleValue());
	}
	
    // SQRT
    class SqrtFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			
			if (context.getParams()[0].isDecimal()) {
				stack.push(sqrt(context, stack.popDecimal()));
				return;
			}

			stack.push(sqrt(context, stack.popDouble()));
			
		}
    	
    }

    
    @TLFunctionAnnotation("Natural logarithm.")
    public static final Double log(TLFunctionCallContext context, double d) {
    	return Math.log(d);
    }
    
    @TLFunctionAnnotation("Natural logarithm. Decimal is converted to double prior to the operation.")
    public static final Double log(TLFunctionCallContext context, BigDecimal d) {
    	return log(context, d.doubleValue());
    }
    	
    // LOG
    class LogFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

    	@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			
    		if (context.getParams()[0].isDecimal()) {
    			stack.push(log(context, stack.popDecimal()));
    			return;
    		}
    		
			stack.push(log(context, stack.popDouble()));
		}
    }
    
    
    @TLFunctionAnnotation("Base 10 logarithm.")
    public static final Double log10(TLFunctionCallContext context, double d) {
    	return Math.log10(d);
    }
    
    @TLFunctionAnnotation("Base 10 logarithm. Decimal is converted into double prior to the operation.")
    public static final Double log10(TLFunctionCallContext context, BigDecimal d) {
    	return log10(context, d.doubleValue());
    }
    	
    // LOG
    class Log10Function implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

    	@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			
			
			if (context.getParams()[0].isDecimal()) {
				stack.push(log10(context, stack.popDecimal()));
				return;
			}
			
			stack.push(log10(context, stack.popDouble()));
		}
    }
    
    
    @TLFunctionAnnotation("Returns Euler's number e raised to the power of a double value.")
    public static final Double exp(TLFunctionCallContext context, Double d) {
    	return Math.exp(d);
    }
    
    @TLFunctionAnnotation("Returns Euler's number e raised to the power of a double value. Decimal is converted to double prior to the operation.")
    public static final Double exp(TLFunctionCallContext context, BigDecimal d) {
    	return exp(context, d.doubleValue());
    }
    
    // EXP
    class ExpFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}
    	
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isDecimal()) {
				stack.push(exp(context, stack.popDecimal()));
				return;
			}
			
			stack.push(exp(context, stack.popDouble()));
			
		}
    }         
    
    @TLFunctionAnnotation("Returns the largest (closest to positive infinity) double value that is less than or equal to the argument and is equal to a mathematical integer.")
    public static final Double floor(TLFunctionCallContext context, double d) {
    	return Math.floor(d);
    }
    
    @TLFunctionAnnotation("Returns the largest (closest to positive infinity) double value that is less than or equal to the argument and is equal to a mathematical integer.")
    public static final BigDecimal floor(TLFunctionCallContext context, BigDecimal d) {
    	return d.setScale(0, RoundingMode.FLOOR);
    }
    
    // FLOOR
    class FloorFunction implements TLFunctionPrototype { 

		@Override
		public void init(TLFunctionCallContext context) {
		}

    	@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			
			if (context.getParams()[0].isDecimal()) {
				stack.push(floor(context, stack.popDecimal()));
				return;
			}
			
			stack.push(floor(context, stack.popDouble()));
		}
    }

    static final long pow10(int exponent){
    	long value=1;
    	while(exponent>0){
    		value*=10l;
    		exponent--;
    	}
    	return value;
    }
    
    
    @TLFunctionAnnotation("Returns double value rounded to specified precision.")
    public static final Double round(TLFunctionCallContext context, double d, int precision) {
    	long multiple=pow10(precision);
    	return ((double)Math.round(d*multiple))/multiple;
    }
    
    @TLFunctionAnnotation("Returns decimal value rounded to specified precision.")
    public static final BigDecimal round(TLFunctionCallContext context, BigDecimal d, int precision) {
    	return d.setScale(precision, RoundingMode.HALF_EVEN); 
    }
    
    @TLFunctionAnnotation("Returns long value closest to the argument.")
    public static final Long round(TLFunctionCallContext context, double d) {
    	return Math.round(d);
    }
    
    @TLFunctionAnnotation("Returns decimal value rounded to the closest integer value.")
    public static final BigDecimal round(TLFunctionCallContext context, BigDecimal d) {
    	return d.setScale(0, RoundingMode.HALF_EVEN); 
    }
    
    // ROUND
	class RoundFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {

			if (context.getParams().length == 2) {
				// Precision has to be popped from stack before the argument to round
				final Integer precision = stack.popInt();
				if (context.getParams()[0].isDecimal()) {
					stack.push(round(context, stack.popDecimal(), precision));
				} else {
					stack.push(round(context, stack.popDouble(), precision));
				}
			} else {
				if (context.getParams()[0].isDecimal()) {
					stack.push(round(context, stack.popDecimal()));
				} else {
					stack.push(round(context, stack.popDouble()));
				}
			}
		}
	}
    
    @TLFunctionAnnotation("Returns the smallest (closest to negative infinity) double value that is greater than or equal to the argument and is equal to a mathematical integer.")
    public static final Double ceil(TLFunctionCallContext context, double d) {
    	return Math.ceil(d);
    }
    
    @TLFunctionAnnotation("Returns the smallest (closest to negative infinity) decimal value that is greater than or equal to the argument and is equal to a mathematical integer.")
    public static final BigDecimal ceil(TLFunctionCallContext context, BigDecimal d) {
    	return d.setScale(0,RoundingMode.CEILING);
    }
    
    // CEIL
    class CeilFunction implements TLFunctionPrototype { 

		@Override
		public void init(TLFunctionCallContext context) {
		}

    	@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			
			if (context.getParams()[0].isDecimal()) {
				stack.push(ceil(context, stack.popDecimal()));
				return;
			}
			
			stack.push(ceil(context, stack.popDouble()));
		}
    }
    
    @TLFunctionAnnotation("Returns the value of the first argument raised to the power of the second argument.")
    public static final Double pow(TLFunctionCallContext context, double argument, double power) {
    	return Math.pow(argument, power);
    }
    
    @TLFunctionAnnotation("Returns the value of the first argument raised to the power of the second argument.")
    public static final BigDecimal pow(TLFunctionCallContext context, BigDecimal argument, BigDecimal power) {
    	return argument.pow(power.intValue());
    }
    
    // POW
    class PowFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isDecimal() || context.getParams()[1].isDecimal()) {
				final BigDecimal pow = stack.popDecimal();
				final BigDecimal arg = stack.popDecimal();
				stack.push(pow(context, arg,pow));
				return;
			}
			
			final Double pow = stack.popDouble();
			final Double arg = stack.popDouble();
			stack.push(pow(context, arg,pow));
		} 
    	
    }                        
    
    @TLFunctionAnnotation("Value of pi constant")
    public static final Double pi(TLFunctionCallContext context) {
    	return Math.PI;
    }
    
    // PI
    class PiFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(pi(context));
		} 
    }         
    
    
    @TLFunctionAnnotation("Value of e function")
    public static final Double e(TLFunctionCallContext context) {
    	return Math.E;
    }
    
    //  E
    class EFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(e(context));
		} 

    }
    
    // ABS
    @TLFunctionAnnotation("Computes absolute value of the argument.")
    public static final Integer abs(TLFunctionCallContext context, Integer i) {
    	return Math.abs(i);
    }
    
    @TLFunctionAnnotation("Computes absolute value of the argument.")
    public static final Long abs(TLFunctionCallContext context, Long l) {
    	return Math.abs(l);
    }
    
    @TLFunctionAnnotation("Computes absolute value of the argument.")
    public static final Double abs(TLFunctionCallContext context, Double d) {
    	return Math.abs(d);
    }
    
    @TLFunctionAnnotation("Computes absolute value of the argument.")
    public static final BigDecimal abs(TLFunctionCallContext context, BigDecimal d) {
    	return d.abs();
    }
    
    class AbsFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isInteger()) {
				stack.push(abs(context, stack.popInt()));
				return;
			} 
			
			if (context.getParams()[0].isLong()) {
				stack.push(abs(context, stack.popLong()));
				return;
			} 
			
			if (context.getParams()[0].isDouble()) {
				stack.push(abs(context, stack.popDouble()));
				return;
			} 
			
			if (context.getParams()[0].isDecimal()) {
				stack.push(abs(context, stack.popDecimal()));
				return;
			}
		} 

    }

 // SIGNUM
    
    @TLFunctionAnnotation("Computes signum value of the argument.")
    public static final Integer signum(TLFunctionCallContext context, Integer i) {
    	return i > 0 ? 1 : i < 0 ? -1 : 0;
    }
    
    @TLFunctionAnnotation("Computes signum value of the argument.")
    public static final Long signum(TLFunctionCallContext context, Long l) {
    	return l>0l ? 1l : l < 0l ? -1l : 0;
    }
    
    @TLFunctionAnnotation("Computes signum value of the argument.")
    public static final Double signum(TLFunctionCallContext context, Double d) {
    	return Math.signum(d);
    }
    
    @TLFunctionAnnotation("Computes signum value of the argument.")
    public static final Integer signum(TLFunctionCallContext context, BigDecimal d) {
    	return d.signum();
    }
    
    class SignumFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isInteger()) {
				stack.push(signum(context, stack.popInt()));
				return;
			} 
			
			if (context.getParams()[0].isLong()) {
				stack.push(signum(context, stack.popLong()));
				return;
			} 
			
			if (context.getParams()[0].isDouble()) {
				stack.push(signum(context, stack.popDouble()));
				return;
			} 
			
			if (context.getParams()[0].isDecimal()) {
				stack.push(signum(context, stack.popDecimal()));
				return;
			}
		} 

    }
    
    
    @TLFunctionAnnotation("Computes bitwise OR of two operands.")
    public static final Long bitOr(TLFunctionCallContext context, Long i, Long j) {
    	return i | j;
    }

    @TLFunctionAnnotation("Computes bitwise OR of two operands.")
    public static final Integer bitOr(TLFunctionCallContext context, Integer i, Integer j) {
    	return i | j;
    }
    
    class BitOrFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		  public void execute(Stack stack, TLFunctionCallContext context) {
		   
		   /*
		    * The variant bitOr(int,int) can only be called when both arguments are int
		    */
		   if (context.getParams()[0].isInteger() && context.getParams()[1].isInteger()) {
		    stack.push(bitOr(context,stack.popInt(), stack.popInt()));
		   } else {
		    
		    /*
		     * In all other cases, i.e. bitOr(int,long), bitOr(long, int), bitOr(long,long)
		     * the compiler will automatically cast int to long so we have to always call bitOr(long,long)
		     */
		    stack.push(bitOr(context, stack.popLong(), stack.popLong()));
		   }
		   
		  } 

    }

    @TLFunctionAnnotation("Computes bitwise AND of two operands.")
    public static final Long bitAnd(TLFunctionCallContext context, Long i, Long j) {
    	return i & j;
    }

	@TLFunctionAnnotation("Computes bitwise AND of two operands.")
	public static final Integer bitAnd(TLFunctionCallContext context, Integer i, Integer j) {
		return i & j;
	}

	class BitAndFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		  public void execute(Stack stack, TLFunctionCallContext context) {
		   
		   /*
		    * The variant bitAnd(int,int) can only be called when both arguments are int
		    */
		   if (context.getParams()[0].isInteger() && context.getParams()[1].isInteger()) {
		    stack.push(bitAnd(context,stack.popInt(), stack.popInt()));
		   } else {
		    
		    /*
		     * In all other cases, i.e. bitAnd(int,long), bitAnd(long, int), bitAnd(long,long)
		     * the compiler will automatically cast int to long so we have to always call bitAnd(long,long)
		     */
		    stack.push(bitAnd(context, stack.popLong(), stack.popLong()));
		   }
		   
		  } 
	}

    @TLFunctionAnnotation("Computes bitwise XOR of two operands.")
    public static final Long bitXor(TLFunctionCallContext context, Long i, Long j) {
    	return i ^ j;
    }

    @TLFunctionAnnotation("Computes bitwise XOR of two operands.")
    public static final Integer bitXor(TLFunctionCallContext context, Integer i, Integer j) {
    	return i ^ j;
    }
    
    class BitXorFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		  public void execute(Stack stack, TLFunctionCallContext context) {
		   
		   /*
		    * The variant bitXor(int,int) can only be called when both arguments are int
		    */
		   if (context.getParams()[0].isInteger() && context.getParams()[1].isInteger()) {
		    stack.push(bitXor(context,stack.popInt(), stack.popInt()));
		   } else {
		    
		    /*
		     * In all other cases, i.e. bitXor(int,long), bitXor(long, int), bitXor(long,long)
		     * the compiler will automatically cast int to long so we have to always call bitXor(long,long)
		     */
		    stack.push(bitXor(context, stack.popLong(), stack.popLong()));
		   }
		   
		  } 
    }
    
    @TLFunctionAnnotation("Shifts the first operand to the left by bits specified in the second operand.")
    public static final Long bitLShift(TLFunctionCallContext context, Long i, Long j) {
    	return i << j;
    }

    @TLFunctionAnnotation("Shifts the first operand to the left by bits specified in the second operand.")
    public static final Integer bitLShift(TLFunctionCallContext context, Integer i, Integer j) {
    	return i << j;
    }
    
    class BitLShiftFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			 /*
			  * The variant bitLShift(int,int) can only be called when both arguments are int
			  */
			if (context.getParams()[0].isInteger() && context.getParams()[1].isInteger()) {
				Integer second = stack.popInt();
				stack.push(bitLShift(context,stack.popInt(), second));
			} else {
			    /*
			     * In all other cases, i.e. bitLShift(int,long), bitLShift(long, int), bitLShift(long,long)
			     * the compiler will automatically cast int to long so we have to always call bitLShift(long,long)
			     */
				Long second = stack.popLong();
				stack.push(bitLShift(context, stack.popLong(), second));
			}
		} 
    }

    @TLFunctionAnnotation("Shifts the first operand to the right by bits specified in the second operand.")
    public static final Long bitRShift(TLFunctionCallContext context, Long i, Long j) {
    	return i >> j;
    }

    @TLFunctionAnnotation("Shifts the first operand to the right by bits specified in the second operand.")
    public static final Integer bitRShift(TLFunctionCallContext context, Integer i, Integer j) {
    	return i >> j;
    }
    
    class BitRShiftFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			 /*
			  * The variant bitRShift(int,int) can only be called when both arguments are int
			  */
			if (context.getParams()[0].isInteger() && context.getParams()[1].isInteger()) {
				Integer second = stack.popInt();
				stack.push(bitRShift(context,stack.popInt(), second));
			} else {
			    /*
			     * In all other cases, i.e. bitRShift(int,long), bitRShift(long, int), bitRShift(long,long)
			     * the compiler will automatically cast int to long so we have to always call bitRShift(long,long)
			     */
				Long second = stack.popLong();
				stack.push(bitRShift(context, stack.popLong(), second));
			}
		} 
    }
    
        
    @TLFunctionAnnotation("Inverts all bits in argument")
    public static final Long bitNegate(TLFunctionCallContext context, Long i) {
    	return ~i;
    }
    
    @TLFunctionAnnotation("Inverts all bits in argument")
    public static final Integer bitNegate(TLFunctionCallContext context, Integer i) {
    	return ~i;
    }
    
    class BitNegateFunction implements TLFunctionPrototype {
    	
		@Override
		public void init(TLFunctionCallContext context) {
		}

    	@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
    		if (context.getParams()[0].isInteger()) {
    			stack.push(bitNegate(context, stack.popInt()));
    			return;
    		} else if (context.getParams()[0].isLong()) {
    			stack.push(bitNegate(context, stack.popLong()));
    			return;
    		}
    	}
    	
    }
    
    

    @TLFunctionAnnotation("Tests if n-th bit of 1st argument is set")
	public static final Boolean bitIsSet(TLFunctionCallContext context, Integer input, Integer bitPosition) {
    	return ((input & ( 1 << bitPosition)) != 0);
	}
    
    @TLFunctionAnnotation("Tests if n-th bit of 1st argument is set")
	public static final Boolean bitIsSet(TLFunctionCallContext context, Long input, Integer bitPosition) {
    	return ((input & ( 1l << bitPosition)) != 0);
	}

    
    class BitIsSetFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isInteger()) {
				int bitPosition = stack.popInt();
				stack.push(bitIsSet(context, stack.popInt(), bitPosition));
			} else if (context.getParams()[0].isLong()) {
				int bitPosition = stack.popInt();
				stack.push(bitIsSet(context, stack.popLong(), bitPosition));
			}
		}
    }
    
    @TLFunctionAnnotation("Sets or resets n-th bit of 1st argument")
	public static final Long bitSet(TLFunctionCallContext context, Long input, Integer bitPosition, boolean value) {
    	Long result;
    	if (value)
    		result = input | (1l << bitPosition); 
		else
			result = input & (~(1l << bitPosition));
    	
    	return result;
    }    	
    
    @TLFunctionAnnotation("Sets or resets n-th bit of 1st argument")
	public static final Integer bitSet(TLFunctionCallContext context, Integer input, Integer bitPosition, boolean value) {
    	Integer result;
    	if (value)
    		result = input | (1 << bitPosition); 
		else
			result = input & (~(1 << bitPosition));
    	
    	return result;
	}


    
    class BitSetFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

    	@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
    		boolean value = stack.popBoolean();
    		int bitPosition = stack.popInt();
			if (context.getParams()[0].isInteger()) {
				stack.push(bitSet(context, stack.popInt(), bitPosition, value));
			} else if (context.getParams()[0].isLong()) {
				stack.push(bitSet(context, stack.popLong(), bitPosition, value));
			}
			
		}
    	
    }
    
// MIN
    
    @TLFunctionAnnotation("Returns min value of the arguments.")
    public static final Integer min(TLFunctionCallContext context, Integer a, Integer b) {
    	return a==null ? b : b==null ? a : Math.min(a, b);
    }
    
    @TLFunctionAnnotation("Returns min value of the arguments.")
    public static final Long min(TLFunctionCallContext context, Long a, Long b) {
    	return a==null ? b : b==null ? a : Math.min(a, b);
    }
    
    @TLFunctionAnnotation("Returns min value of the arguments.")
    public static final Double min(TLFunctionCallContext context, Double a, Double b) {
    	return a==null ? b : b==null ? a : Math.min(a, b);
    }
    
    @TLFunctionAnnotation("Returns min value of the arguments.")
    public static final BigDecimal min(TLFunctionCallContext context, BigDecimal a, BigDecimal b) {
    	return a==null ? b : b==null ? a : a.min(b);
    }
    
    
	@TLFunctionAnnotation("Returns min value of the array.")
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static final <E> E min(TLFunctionCallContext context, List <E> vals) {
		Iterator i = vals.iterator();
		Comparable candidate = (Comparable) i.next();

		while (i.hasNext()) {
			Comparable next = (Comparable) i.next();
			if (next==null) continue;
			if (candidate==null)
				candidate=next;
			else if (next.compareTo(candidate) < 0)
				candidate = next;
		}
		return (E)candidate;
	}
    
    class MinFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isInteger()) {
				stack.push(min(context, stack.popInt(),stack.popInt()));
				return;
			} 
			
			if (context.getParams()[0].isLong()) {
				stack.push(min(context, stack.popLong(),stack.popLong()));
				return;
			} 
			
			if (context.getParams()[0].isDouble()) {
				stack.push(min(context, stack.popDouble(),stack.popDouble()));
				return;
			} 
			
			if (context.getParams()[0].isDecimal()) {
				stack.push(min(context, stack.popDecimal(),stack.popDecimal()));
				return;
			}
			if (context.getParams()[0].isList()) {
				stack.push(min(context, stack.popList()));
				return;
			}
		} 

    }
    
// MAX
    
    @TLFunctionAnnotation("Returns max value of the arguments.")
    public static final Integer max(TLFunctionCallContext context, Integer a, Integer b) {
    	return a==null ? b : b==null ? a : Math.max(a, b);
    }
    
    @TLFunctionAnnotation("Returns max value of the arguments.")
    public static final Long max(TLFunctionCallContext context, Long a, Long b) {
    	return a==null ? b : b==null ? a : Math.max(a, b);
    }
    
    @TLFunctionAnnotation("Returns max value of the arguments.")
    public static final Double max(TLFunctionCallContext context, Double a, Double b) {
    	return a==null ? b : b==null ? a : Math.max(a, b);
    }
    
    @TLFunctionAnnotation("Returns max value of the arguments.")
    public static final BigDecimal max(TLFunctionCallContext context, BigDecimal a, BigDecimal b) {
    	return a==null ? b : b==null ? a : a.max(b);
    }
    
    @TLFunctionAnnotation("Returns max value of the array.")
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static final <E> E max(TLFunctionCallContext context, List <E> vals) {
    	Iterator i = vals.iterator();
		Comparable candidate = (Comparable) i.next();

		while (i.hasNext()) {
			Comparable next = (Comparable) i.next();
			if (next==null) continue;
			if (candidate==null)
				candidate=next;
			else if (next.compareTo(candidate) > 0)
				candidate = next;
		}
		return (E)candidate;
    }
    
    class MaxFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isInteger()) {
				stack.push(max(context, stack.popInt(),stack.popInt()));
				return;
			} 
			
			if (context.getParams()[0].isLong()) {
				stack.push(max(context, stack.popLong(),stack.popLong()));
				return;
			} 
			
			if (context.getParams()[0].isDouble()) {
				stack.push(max(context, stack.popDouble(),stack.popDouble()));
				return;
			} 
			
			if (context.getParams()[0].isDecimal()) {
				stack.push(max(context, stack.popDecimal(),stack.popDecimal()));
				return;
			}
			if (context.getParams()[0].isList()) {
				stack.push(max(context, stack.popList()));
				return;
			}
		} 

    }
    
    @TLFunctionAnnotation("Calculates sin value of parameter.")
    public static final Double sin(TLFunctionCallContext context, Double d) {
    	return Math.sin(d);
    }
    
    @TLFunctionAnnotation("Calculates sin value of parameter. Decimal is converted to double prior to the operation.")
    public static final Double sin(TLFunctionCallContext context, BigDecimal d) {
    	return sin(context, d.doubleValue());
    }
    
    // SIN
    class SinFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}
    	
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isDecimal()) {
				stack.push(sin(context, stack.popDecimal()));
				return;
			}
			
			stack.push(sin(context, stack.popDouble()));
			
		}
    }         
    
    @TLFunctionAnnotation("Calculates cos value of parameter.")
    public static final Double cos(TLFunctionCallContext context, Double d) {
    	return Math.cos(d);
    }
    
    @TLFunctionAnnotation("Calculates cos value of parameter. Decimal is converted to double prior to the operation.")
    public static final Double cos(TLFunctionCallContext context, BigDecimal d) {
    	return cos(context, d.doubleValue());
    }
    
    // COS
    class CosFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}
    	
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isDecimal()) {
				stack.push(cos(context, stack.popDecimal()));
				return;
			}
			
			stack.push(cos(context, stack.popDouble()));
			
		}
    }         
    
    @TLFunctionAnnotation("Calculates tan value of parameter.")
    public static final Double tan(TLFunctionCallContext context, Double d) {
    	return Math.tan(d);
    }
    
    @TLFunctionAnnotation("Calculates tan value of parameter. Decimal is converted to double prior to the operation.")
    public static final Double tan(TLFunctionCallContext context, BigDecimal d) {
    	return tan(context, d.doubleValue());
    }
    
    // TAN
    class TanFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}
    	
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isDecimal()) {
				stack.push(tan(context, stack.popDecimal()));
				return;
			}
			
			stack.push(tan(context, stack.popDouble()));
			
		}
    }
    
    @TLFunctionAnnotation("Calculates asin value of parameter.")
    public static final Double asin(TLFunctionCallContext context, Double d) {
    	return Math.asin(d);
    }
    
    @TLFunctionAnnotation("Calculates asin value of parameter. Decimal is converted to double prior to the operation.")
    public static final Double asin(TLFunctionCallContext context, BigDecimal d) {
    	return asin(context, d.doubleValue());
    }
    
    // ASIN
    class ASinFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}
    	
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isDecimal()) {
				stack.push(asin(context, stack.popDecimal()));
				return;
			}
			
			stack.push(asin(context, stack.popDouble()));
			
		}
    }
    
    @TLFunctionAnnotation("Calculates acos value of parameter.")
    public static final Double acos(TLFunctionCallContext context, Double d) {
    	return Math.acos(d);
    }
    
    @TLFunctionAnnotation("Calculates acos value of parameter. Decimal is converted to double prior to the operation.")
    public static final Double acos(TLFunctionCallContext context, BigDecimal d) {
    	return acos(context, d.doubleValue());
    }
    
    // ACOS
    class ACosFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}
    	
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isDecimal()) {
				stack.push(acos(context, stack.popDecimal()));
				return;
			}
			
			stack.push(acos(context, stack.popDouble()));
			
		}
    }
    
    @TLFunctionAnnotation("Calculates atan value of parameter.")
    public static final Double atan(TLFunctionCallContext context, Double d) {
    	return Math.atan(d);
    }
    
    @TLFunctionAnnotation("Calculates atan value of parameter. Decimal is converted to double prior to the operation.")
    public static final Double atan(TLFunctionCallContext context, BigDecimal d) {
    	return atan(context, d.doubleValue());
    }
    
    // ATAN
    class ATanFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}
    	
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isDecimal()) {
				stack.push(atan(context, stack.popDecimal()));
				return;
			}
			
			stack.push(atan(context, stack.popDouble()));
			
		}
    }
    
    @TLFunctionAnnotation("Converts input value from radians to derees.")
    public static final Double toDegrees(TLFunctionCallContext context, Double d) {
    	return Math.toDegrees(d);
    }
    
    @TLFunctionAnnotation("Converts input value from radians to derees. Decimal is converted to double prior to the operation.")
    public static final Double toDegrees(TLFunctionCallContext context, BigDecimal d) {
    	return toDegrees(context, d.doubleValue());
    }
    
    // TO DEGREES
    class ToDegreesFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}
    	
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isDecimal()) {
				stack.push(toDegrees(context, stack.popDecimal()));
				return;
			}
			
			stack.push(toDegrees(context, stack.popDouble()));
			
		}
    }
    
    @TLFunctionAnnotation("Converts input value from degrees to radians.")
    public static final Double toRadians(TLFunctionCallContext context, Double d) {
    	return Math.toRadians(d);
    }
    
    @TLFunctionAnnotation("Converts input value from degrees to radians. Decimal is converted to double prior to the operation.")
    public static final Double toRadians(TLFunctionCallContext context, BigDecimal d) {
    	return toRadians(context, d.doubleValue());
    }
    
    // TO RADIANS
    class ToRadiansFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}
    	
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isDecimal()) {
				stack.push(toRadians(context, stack.popDecimal()));
				return;
			}
			
			stack.push(toRadians(context, stack.popDouble()));
			
		}
    }
}
