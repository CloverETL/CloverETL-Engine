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
			"bitOr".equals(functionName) ? new BitOrFunction() :
			"bitAnd".equals(functionName) ? new BitAndFunction() :
			"bitXor".equals(functionName) ? new BitXorFunction() :
			"bitLShift".equals(functionName) ? new BitLShiftFunction() :
			"bitRSshift".equals(functionName) ? new BitRShiftFunction() :
			"bitNegate".equals(functionName) ? new BitNegateFunction() :
			"bitSet".equals(functionName) ? new BitSetFunction() :
			"bitIsSet".equals(functionName) ? new BitIsSetFunction() :
			"randomGaussian".equals(functionName) ? new RandomGaussianFunction() :
		    "randomBoolean".equals(functionName) ? new RandomBooleanFunction() : 
		    "randomInteger".equals(functionName) ? new RandomIntegerFunction() :
		    "randomLong".equals(functionName) ? new RandomLongFunction() :
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
    	return ((TLDataGeneratorCache)context.getCache()).dataGenerator.nextDouble();
    }
    
    @TLFunctionAnnotation("Random number (>=0, <1). Allows changing seed")
    public static final Double random(TLFunctionCallContext context, Long seed) {
    	DataGenerator generator = ((TLDataGeneratorCache)context.getCache()).dataGenerator;
    	generator.setSeed(seed);
    	return generator.nextDouble();
    }
    
    // RANDOM
    class RandomFunction implements TLFunctionPrototype {
    	
		public void init(TLFunctionCallContext context) {
			context.setCache(new TLDataGeneratorCache());
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
    public static final Long bitOr(TLFunctionCallContext context, Long i, Long j) {
    	return i | j;
    }

    @TLFunctionAnnotation("Computes bitwise OR of two operands.")
    public static final Integer bitOr(TLFunctionCallContext context, Integer i, Integer j) {
    	return i | j;
    }
    
    class BitOrFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isInteger() && context.getParams()[1].isInteger()) {
				stack.push(bitOr(context, stack.popInt(), stack.popInt()));
				return;
			} 
			
			if (context.getParams()[0].isLong() && context.getParams()[1].isLong()) {
				stack.push(bitOr(context, stack.popLong(), stack.popLong()));
				return;
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

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isInteger() && context.getParams()[1].isInteger()) {
				stack.push(bitAnd(context, stack.popInt(), stack.popInt()));
				return;
			}

			if (context.getParams()[0].isLong() && context.getParams()[1].isLong()) {
				stack.push(bitAnd(context, stack.popLong(), stack.popLong()));
				return;
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

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isInteger() && context.getParams()[1].isInteger()) {
				stack.push(bitXor(context, stack.popInt(), stack.popInt()));
				return;
			} 
			
			if (context.getParams()[0].isLong() && context.getParams()[1].isLong()) {
				stack.push(bitXor(context, stack.popLong(), stack.popLong()));
				return;
			} 
		} 
    }
    
    @TLFunctionAnnotation("Shifts the first operand to the left by bits specified in the second operand.")
    public static final Long bitLShift(TLFunctionCallContext context, Long j, Long i) {
    	return i << j;
    }

    @TLFunctionAnnotation("Shifts the first operand to the left by bits specified in the second operand.")
    public static final Integer bitLShift(TLFunctionCallContext context, Integer j, Integer i) {
    	return i << j;
    }
    
    class BitLShiftFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isInteger() && context.getParams()[1].isInteger()) {
				stack.push(bitLShift(context, stack.popInt(), stack.popInt()));
				return;
			} 
			
			if (context.getParams()[0].isLong() && context.getParams()[1].isLong()) {
				stack.push(bitLShift(context, stack.popLong(), stack.popLong()));
				return;
			} 
		} 
    }

    @TLFunctionAnnotation("Shifts the first operand to the right by bits specified in the second operand.")
    public static final Long bitRShift(TLFunctionCallContext context, Long j, Long i) {
    	return i >> j;
    }

    @TLFunctionAnnotation("Shifts the first operand to the right by bits specified in the second operand.")
    public static final Integer bitRShift(TLFunctionCallContext context, Integer j, Integer i) {
    	return i >> j;
    }
    
    class BitRShiftFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isInteger() && context.getParams()[1].isInteger()) {
				stack.push(bitRShift(context, stack.popInt(), stack.popInt()));
				return;
			} 
			
			if (context.getParams()[0].isLong() && context.getParams()[1].isLong()) {
				stack.push(bitRShift(context, stack.popLong(), stack.popLong()));
				return;
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
    	
		public void init(TLFunctionCallContext context) {
		}

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

		public void init(TLFunctionCallContext context) {
		}

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

		public void init(TLFunctionCallContext context) {
		}

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
    
    @TLFunctionAnnotation("Random Gaussian number.")
    public static final Double randomGaussian(TLFunctionCallContext context) {
    	return getGenerator(Thread.currentThread()).nextGaussian();
    }
    
    @TLFunctionAnnotation("Random Gaussian number. Allows changing seed.")
    public static final Double randomGaussian(TLFunctionCallContext context, Long seed) {
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
				stack.push(randomGaussian(context, stack.popLong()));
			} else {
				stack.push(randomGaussian(context));
			}
		}
    }
    
    @TLFunctionAnnotation("Random boolean.")
    public static final Boolean randomBoolean(TLFunctionCallContext context) {
    	return getGenerator(Thread.currentThread()).nextBoolean();
    }
    
    @TLFunctionAnnotation("Random boolean. Allows changing seed.")
    public static final Boolean randomBoolean(TLFunctionCallContext context, Long seed) {
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
				stack.push(randomBoolean(context, stack.popLong()));
			} else {
				stack.push(randomBoolean(context));
			}
		}
    }
    
    @TLFunctionAnnotation("Random integer.")
    public static final Integer randomInteger(TLFunctionCallContext context) {
    	return getGenerator(Thread.currentThread()).nextInt();
    }
    
    @TLFunctionAnnotation("Random integer. Allows changing seed.")
    public static final Integer randomInteger(TLFunctionCallContext context, Long seed) {
    	DataGenerator generator = getGenerator(Thread.currentThread());
    	generator.setSeed(seed);
    	return generator.nextInt();
    }
    
    @TLFunctionAnnotation("Random integer. Allows changing start and end value.")
    public static final Integer randomInteger(TLFunctionCallContext context, Integer min, Integer max) {
    	return getGenerator(Thread.currentThread()).nextInt(min, max);
    }
    
    @TLFunctionAnnotation("Random integer. Allows changing start, end value and seed.")
    public static final Integer randomInteger(TLFunctionCallContext context, Integer min, Integer max, Long seed) {
    	DataGenerator generator = getGenerator(Thread.currentThread());
    	generator.setSeed(seed);
    	return generator.nextInt(min, max);
    }
    
    // RANDOMINTEGER
    class RandomIntegerFunction implements TLFunctionPrototype {
    	
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
					stack.push(randomInteger(context, min, max, seed));
					break;
				case 2:
					max = stack.popInt();
					min = stack.popInt();
					stack.push(randomInteger(context, min, max));
					break;
				case 1:
					seed = stack.popLong();
					stack.push(randomInteger(context, seed));
					break;
				case 0:
					stack.push(randomInteger(context));
					break;
			}
		}
    }

    @TLFunctionAnnotation("Random long.")
    public static final Long randomLong(TLFunctionCallContext context) {
    	return getGenerator(Thread.currentThread()).nextLong();
    }
    
    @TLFunctionAnnotation("Random long. Allows changing seed.")
    public static final Long randomLong(TLFunctionCallContext context, Long seed) {
    	DataGenerator generator = getGenerator(Thread.currentThread());
    	generator.setSeed(seed);
    	return generator.nextLong();
    }
    
    @TLFunctionAnnotation("Random long. Allows changing start and end value.")
    public static final Long randomLong(TLFunctionCallContext context, Long min, Long max) {
    	return getGenerator(Thread.currentThread()).nextLong(min, max);
    }
    
    @TLFunctionAnnotation("Random long. Allows changing start, end value and seed.")
    public static final Long randomLong(TLFunctionCallContext context, Long min, Long max, Long seed) {
    	DataGenerator generator = getGenerator(Thread.currentThread());
    	generator.setSeed(seed);
    	return generator.nextLong(min, max);
    }
    
    // RANDOMLONG
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
					stack.push(randomLong(context, min, max, seed));
					break;
				case 2:
					max = stack.popLong();
					min = stack.popLong();
					stack.push(randomLong(context, min, max));
					break;
				case 1:
					seed = stack.popLong();
					stack.push(randomLong(context, seed));
					break;
				case 0:
					stack.push(randomLong(context));
					break;
			}
		}
    }
}
