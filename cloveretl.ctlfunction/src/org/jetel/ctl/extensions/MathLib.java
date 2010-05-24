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
import java.util.HashMap;
import java.util.Map;

import org.jetel.ctl.Stack;
import org.jetel.util.DataGenerator;


public class MathLib extends TLFunctionLibrary {
    
	private static Map<Thread, DataGenerator> dataGenerators = new HashMap<Thread, DataGenerator>();
	
	private static synchronized DataGenerator getGenerator(Thread key) {
    	DataGenerator generator = dataGenerators.get(key);
    	if (generator == null) {
    		generator = new DataGenerator();
    		dataGenerators.put(key, generator);
    	}
    	return generator;
    }
	
	@Override
	public TLFunctionPrototype getExecutable(String functionName) {
		TLFunctionPrototype ret = 
			"sqrt".equals(functionName) ? new SqrtFunction() :
			"log".equals(functionName) ? new LogFunction() :
			"log10".equals(functionName) ? new Log10Function() :
			"exp".equals(functionName) ? new ExpFunction() :
			"round".equals(functionName) ? new RoundFunction() :
			"pow".equals(functionName) ? new PowFunction() :
			"pi".equals(functionName) ? new PiFunction() :
			"e".equals(functionName) ? new EFunction() :
			"random".equals(functionName) ? new RandomFunction() :
			"abs".equals(functionName) ? new AbsFunction() :
			"bit_or".equals(functionName) ? new BitOrFunction() :
			"bit_and".equals(functionName) ? new BitAndFunction() :
			"bit_xor".equals(functionName) ? new BitXorFunction() :
			"bit_lshift".equals(functionName) ? new BitLShiftFunction() :
			"bit_rshift".equals(functionName) ? new BitRShiftFunction() :
			"bit_negate".equals(functionName) ? new BitNegateFunction() :
			"bit_set".equals(functionName) ? new BitSetFunction() :
			"bit_is_set".equals(functionName) ? new BitIsSetFunction() :
			"random_gaussian".equals(functionName) ? new RandomGaussianFunction() :
		    "random_boolean".equals(functionName) ? new RandomBooleanFunction() : 
		    "random_int".equals(functionName) ? new RandomIntFunction() :
		    "random_long".equals(functionName) ? new RandomLongFunction() :
			null;
			
		if (ret == null) {
    		throw new IllegalArgumentException("Unknown function '" + functionName + "'");
    	}

		return ret;
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

		public void init(TLFunctionCallContext context) {
		}

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

		public void init(TLFunctionCallContext context) {
		}

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

		public void init(TLFunctionCallContext context) {
		}

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

		public void init(TLFunctionCallContext context) {
		}
    	
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isDecimal()) {
				stack.push(exp(context, stack.popDecimal()));
				return;
			}
			
			stack.push(exp(context, stack.popDouble()));
			
		}
    }         
    

    @TLFunctionAnnotation("Returns long value closest to the argument.")
    public static final Long round(TLFunctionCallContext context, double d) {
    	return Math.round(d);
    }
    
    @TLFunctionAnnotation("Returns long value closest to the argument. Decimal is converted into double prior to the operation.")
    public static final Long round(TLFunctionCallContext context, BigDecimal d) {
    	return round(context, d.doubleValue()); 
    }
    
    // ROUND
    class RoundFunction implements TLFunctionPrototype { 

		public void init(TLFunctionCallContext context) {
		}

    	public void execute(Stack stack, TLFunctionCallContext context) {
			
			if (context.getParams()[0].isDecimal()) {
				stack.push(round(context, stack.popDecimal()));
				return;
			}
			
			stack.push(round(context, stack.popDouble()));
		}
    }                        
    
    @TLFunctionAnnotation("Returns the value of the first argument raised to the power of the second argument.")
    public static final Double pow(TLFunctionCallContext context, double argument, double power) {
    	return Math.pow(argument, power);
    }
    
    @TLFunctionAnnotation("Returns the value of the first argument raised to the power of the second argument.")
    public static final Double pow(TLFunctionCallContext context, BigDecimal argument, BigDecimal power) {
    	return Math.pow(argument.doubleValue(), power.doubleValue());
    }
    
    // POW
    class PowFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

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

		public void init(TLFunctionCallContext context) {
		}

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

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(e(context));
		} 

    }     

    @TLFunctionAnnotation("Random number (>=0, <1)")
    public static final Double random(TLFunctionCallContext context) {
    	return Math.random();
    }
    
    // RANDOM
    class RandomFunction implements TLFunctionPrototype {
    	
		public void init(TLFunctionCallContext context) {
		}

    	public void execute(Stack stack, TLFunctionCallContext context) {
    		stack.push(random(context));
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

		public void init(TLFunctionCallContext context) {
		}

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

    @TLFunctionAnnotation("Computes bitwise OR of two operands.")
    public static final Long bit_or(TLFunctionCallContext context, Long i, Long j) {
    	return i | j;
    }

    @TLFunctionAnnotation("Computes bitwise OR of two operands.")
    public static final Integer bit_or(TLFunctionCallContext context, Integer i, Integer j) {
    	return i | j;
    }
    
    class BitOrFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isInteger() && context.getParams()[1].isInteger()) {
				stack.push(bit_or(context, stack.popInt(), stack.popInt()));
				return;
			} 
			
			if (context.getParams()[0].isLong() && context.getParams()[1].isLong()) {
				stack.push(bit_or(context, stack.popLong(), stack.popLong()));
				return;
			} 
			
		} 

    }

    @TLFunctionAnnotation("Computes bitwise AND of two operands.")
    public static final Long bit_and(TLFunctionCallContext context, Long i, Long j) {
    	return i & j;
    }

    @TLFunctionAnnotation("Computes bitwise AND of two operands.")
    public static final Integer bit_and(TLFunctionCallContext context, Integer i, Integer j) {
    	return i & j;
    }
    
    class BitAndFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isInteger() && context.getParams()[1].isInteger()) {
				stack.push(bit_and(context, stack.popInt(), stack.popInt()));
				return;
			} 
			
			if (context.getParams()[0].isLong() && context.getParams()[1].isLong()) {
				stack.push(bit_and(context, stack.popLong(), stack.popLong()));
				return;
			} 
			
		} 

    }

    @TLFunctionAnnotation("Computes bitwise AND of two operands.")
    public static final Long bit_xor(TLFunctionCallContext context, Long i, Long j) {
    	return i ^ j;
    }

    @TLFunctionAnnotation("Computes bitwise AND of two operands.")
    public static final Integer bit_xor(TLFunctionCallContext context, Integer i, Integer j) {
    	return i ^ j;
    }
    
    class BitXorFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isInteger() && context.getParams()[1].isInteger()) {
				stack.push(bit_xor(context, stack.popInt(), stack.popInt()));
				return;
			} 
			
			if (context.getParams()[0].isLong() && context.getParams()[1].isLong()) {
				stack.push(bit_xor(context, stack.popLong(), stack.popLong()));
				return;
			} 
		} 
    }
    
    @TLFunctionAnnotation("Shifts the first operand to the left by bits specified in the second operand.")
    public static final Long bit_lshift(TLFunctionCallContext context, Long j, Long i) {
    	return i << j;
    }

    @TLFunctionAnnotation("Shifts the first operand to the left by bits specified in the second operand.")
    public static final Integer bit_lshift(TLFunctionCallContext context, Integer j, Integer i) {
    	return i << j;
    }
    
    class BitLShiftFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isInteger() && context.getParams()[1].isInteger()) {
				stack.push(bit_lshift(context, stack.popInt(), stack.popInt()));
				return;
			} 
			
			if (context.getParams()[0].isLong() && context.getParams()[1].isLong()) {
				stack.push(bit_lshift(context, stack.popLong(), stack.popLong()));
				return;
			} 
		} 
    }

    @TLFunctionAnnotation("Shifts the first operand to the right by bits specified in the second operand.")
    public static final Long bit_rshift(TLFunctionCallContext context, Long j, Long i) {
    	return i >> j;
    }

    @TLFunctionAnnotation("Shifts the first operand to the right by bits specified in the second operand.")
    public static final Integer bit_rshift(TLFunctionCallContext context, Integer j, Integer i) {
    	return i >> j;
    }
    
    class BitRShiftFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isInteger() && context.getParams()[1].isInteger()) {
				stack.push(bit_rshift(context, stack.popInt(), stack.popInt()));
				return;
			} 
			
			if (context.getParams()[0].isLong() && context.getParams()[1].isLong()) {
				stack.push(bit_rshift(context, stack.popLong(), stack.popLong()));
				return;
			} 
		} 
    }
    
        
    @TLFunctionAnnotation("Inverts all bits in argument")
    public static final Long bit_negate(TLFunctionCallContext context, Long i) {
    	return ~i;
    }
    
    @TLFunctionAnnotation("Inverts all bits in argument")
    public static final Integer bit_negate(TLFunctionCallContext context, Integer i) {
    	return ~i;
    }
    
    class BitNegateFunction implements TLFunctionPrototype {
    	
		public void init(TLFunctionCallContext context) {
		}

    	public void execute(Stack stack, TLFunctionCallContext context) {
    		if (context.getParams()[0].isInteger()) {
    			stack.push(bit_negate(context, stack.popInt()));
    			return;
    		} else if (context.getParams()[0].isLong()) {
    			stack.push(bit_negate(context, stack.popLong()));
    			return;
    		}
    	}
    	
    }

    @TLFunctionAnnotation("Tests if n-th bit of 1st argument is set")
	public static final Boolean bit_is_set(TLFunctionCallContext context, Integer input, Integer bitPosition) {
    	return ((input & ( 1 << bitPosition)) != 0);
	}
    
    @TLFunctionAnnotation("Tests if n-th bit of 1st argument is set")
	public static final Boolean bit_is_set(TLFunctionCallContext context, Long input, Integer bitPosition) {
    	return ((input & ( 1l << bitPosition)) != 0);
	}

    
    class BitIsSetFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isInteger()) {
				int bitPosition = stack.popInt();
				stack.push(bit_is_set(context, stack.popInt(), bitPosition));
			} else if (context.getParams()[0].isLong()) {
				int bitPosition = stack.popInt();
				stack.push(bit_is_set(context, stack.popLong(), bitPosition));
			}
			
		}

    }
    
    @TLFunctionAnnotation("Tests if n-th bit of 1st argument is set")
	public static final Long bit_set(TLFunctionCallContext context, Long input, Integer bitPosition, boolean value) {
    	Long result;
    	if (value)
    		result = input | (1l << bitPosition); 
		else
			result = input & (~(1l << bitPosition));
    	
    	return result;
    }    	
    
    @TLFunctionAnnotation("Tests if n-th bit of 1st argument is set")
	public static final Integer bit_set(TLFunctionCallContext context, Integer input, Integer bitPosition, boolean value) {
    	Integer result;
    	if (value)
    		result = input | (1 << bitPosition); 
		else
			result = input & (~(1 << bitPosition));
    	
    	return result;
	}


    
    class BitSetFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

    	public void execute(Stack stack, TLFunctionCallContext context) {
    		boolean value = stack.popBoolean();
    		int bitPosition = stack.popInt();
			if (context.getParams()[0].isInteger()) {
				stack.push(bit_set(context, stack.popInt(), bitPosition, value));
			} else if (context.getParams()[0].isLong()) {
				stack.push(bit_set(context, stack.popLong(), bitPosition, value));
			}
			
		}
    	
    }

    
//    @TLFunctionAnnotation("Sets or resets n-th bit of 1st argument")
    
    @TLFunctionAnnotation("Random Gaussian number.")
    public static final Double random_gaussian(TLFunctionCallContext context) {
    	return getGenerator(Thread.currentThread()).nextGaussian();
    }
    
    @TLFunctionAnnotation("Random Gaussian number. Allows changing seed.")
    public static final Double random_gaussian(TLFunctionCallContext context, Long seed) {
    	DataGenerator generator = getGenerator(Thread.currentThread());
    	generator.setSeed(seed);
    	return generator.nextGaussian();
    }
    
    // RANDOM Gaussian
    class RandomGaussianFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams().length == 1) {
				stack.push(random_gaussian(context, stack.popLong()));
			} else {
				stack.push(random_gaussian(context));
			}
		}
    }
    
    @TLFunctionAnnotation("Random boolean.")
    public static final Boolean random_boolean(TLFunctionCallContext context) {
    	return getGenerator(Thread.currentThread()).nextBoolean();
    }
    
    @TLFunctionAnnotation("Random boolean. Allows changing seed.")
    public static final Boolean random_boolean(TLFunctionCallContext context, Long seed) {
    	DataGenerator generator = getGenerator(Thread.currentThread());
    	generator.setSeed(seed);
    	return generator.nextBoolean();
    }
    // RANDOM Boolean
    class RandomBooleanFunction implements TLFunctionPrototype {
        
		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams().length == 1) {
				stack.push(random_boolean(context, stack.popLong()));
			} else {
				stack.push(random_boolean(context));
			}
		}
    }
    
    @TLFunctionAnnotation("Random integer.")
    public static final Integer random_int(TLFunctionCallContext context) {
    	return getGenerator(Thread.currentThread()).nextInt();
    }
    
    @TLFunctionAnnotation("Random integer. Allows changing seed.")
    public static final Integer random_int(TLFunctionCallContext context, Long seed) {
    	DataGenerator generator = getGenerator(Thread.currentThread());
    	generator.setSeed(seed);
    	return generator.nextInt();
    }
    
    @TLFunctionAnnotation("Random integer. Allows changing start and end value.")
    public static final Integer random_int(TLFunctionCallContext context, Integer min, Integer max) {
    	return getGenerator(Thread.currentThread()).nextInt(min, max);
    }
    
    @TLFunctionAnnotation("Random integer. Allows changing start, end value and seed.")
    public static final Integer random_int(TLFunctionCallContext context, Integer min, Integer max, Long seed) {
    	DataGenerator generator = getGenerator(Thread.currentThread());
    	generator.setSeed(seed);
    	return generator.nextInt(min, max);
    }
    
    // RANDOM_INT
    class RandomIntFunction implements TLFunctionPrototype {
    	
		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			Long seed;
			Integer max;
			Integer min;
			switch (context.getParams().length) {
				case 3:
					seed = stack.popLong();
					max = stack.popInt();
					min = stack.popInt();
					stack.push(random_int(context, min, max, seed));
					break;
				case 2:
					max = stack.popInt();
					min = stack.popInt();
					stack.push(random_int(context, min, max));
					break;
				case 1:
					seed = stack.popLong();
					stack.push(random_int(context, seed));
					break;
				case 0:
					stack.push(random_int(context));
					break;
			}
		}
    }

    @TLFunctionAnnotation("Random long.")
    public static final Long random_long(TLFunctionCallContext context) {
    	return getGenerator(Thread.currentThread()).nextLong();
    }
    
    @TLFunctionAnnotation("Random long. Allows changing seed.")
    public static final Long random_long(TLFunctionCallContext context, Long seed) {
    	DataGenerator generator = getGenerator(Thread.currentThread());
    	generator.setSeed(seed);
    	return generator.nextLong();
    }
    
    @TLFunctionAnnotation("Random long. Allows changing start and end value.")
    public static final Long random_long(TLFunctionCallContext context, Long min, Long max) {
    	return getGenerator(Thread.currentThread()).nextLong(min, max);
    }
    
    @TLFunctionAnnotation("Random long. Allows changing start, end value and seed.")
    public static final Long random_long(TLFunctionCallContext context, Long min, Long max, Long seed) {
    	DataGenerator generator = getGenerator(Thread.currentThread());
    	generator.setSeed(seed);
    	return generator.nextLong(min, max);
    }
    
    // RANDOM_LONG
    class RandomLongFunction implements TLFunctionPrototype {
        
		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			Long seed;
			Long max;
			Long min;
			switch (context.getParams().length) {
				case 3:
					seed = stack.popLong();
					max = stack.popLong();
					min = stack.popLong();
					stack.push(random_long(context, min, max, seed));
					break;
				case 2:
					max = stack.popLong();
					min = stack.popLong();
					stack.push(random_long(context, min, max));
					break;
				case 1:
					seed = stack.popLong();
					stack.push(random_long(context, seed));
					break;
				case 0:
					stack.push(random_long(context));
					break;
			}
		}
    }
}
