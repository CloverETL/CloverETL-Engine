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
import org.jetel.ctl.data.TLType;
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
			/*TODO: bit_is_set bit_set*/
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
	public static final Double sqrt(double i) {
		return Math.sqrt(i);
	}
	
	@TLFunctionAnnotation("Square root. Decimal is converted to double prior to the operation.")
	public static final Double sqrt(BigDecimal b) {
		return sqrt(b.doubleValue());
	}
	
    // SQRT
    class SqrtFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			
			if (actualParams[0].isDecimal()) {
				stack.push(sqrt(stack.popDecimal()));
				return;
			}

			stack.push(sqrt(stack.popDouble()));
			
		}
    	
    }

    
    @TLFunctionAnnotation("Natural logarithm.")
    public static final Double log(double d) {
    	return Math.log(d);
    }
    
    @TLFunctionAnnotation("Natural logarithm. Decimal is converted to double prior to the operation.")
    public static final Double log(BigDecimal d) {
    	return log(d.doubleValue());
    }
    	
    // LOG
    class LogFunction implements TLFunctionPrototype {
    	public void execute(Stack stack, TLType[] actualParams) {
			
    		if (actualParams[0].isDecimal()) {
    			stack.push(log(stack.popDecimal()));
    			return;
    		}
    		
			stack.push(log(stack.popDouble()));
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
			
			
			if (actualParams[0].isDecimal()) {
				stack.push(log10(stack.popDecimal()));
				return;
			}
			
			stack.push(log10(stack.popDouble()));
		}
    }
    
    
    @TLFunctionAnnotation("Returns Euler's number e raised to the power of a double value.")
    public static final Double exp(Double d) {
    	return Math.exp(d);
    }
    
    @TLFunctionAnnotation("Returns Euler's number e raised to the power of a double value. Decimal is converted to double prior to the operation.")
    public static final Double exp(BigDecimal d) {
    	return exp(d.doubleValue());
    }
    
    // EXP
    class ExpFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			if (actualParams[0].isDecimal()) {
				stack.push(exp(stack.popDecimal()));
				return;
			}
			
			stack.push(exp(stack.popDouble()));
			
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
    
    @TLFunctionAnnotation("Value of pi constant")
    public static final Double pi() {
    	return Math.PI;
    }
    
    // PI
    class PiFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(pi());
		} 
    }         
    
    
    @TLFunctionAnnotation("Value of e function")
    public static final Double e() {
    	return Math.E;
    }
    
    //  E
    class EFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(e());
		} 

    }     

    @TLFunctionAnnotation("Random number (>=0, <1)")
    public static final Double random() {
    	return Math.random();
    }
    
    // RANDOM
    class RandomFunction implements TLFunctionPrototype {
    	public void execute(Stack stack, TLType[] actualParams) {
    		stack.push(random());
    	}
    }
    
    // ABS
    @TLFunctionAnnotation("Computes absolute value of the argument.")
    public static final Integer abs(Integer i) {
    	return Math.abs(i);
    }
    
    @TLFunctionAnnotation("Computes absolute value of the argument.")
    public static final Long abs(Long l) {
    	return Math.abs(l);
    }
    
    @TLFunctionAnnotation("Computes absolute value of the argument.")
    public static final Double abs(Double d) {
    	return Math.abs(d);
    }
    
    @TLFunctionAnnotation("Computes absolute value of the argument.")
    public static final BigDecimal abs(BigDecimal d) {
    	return d.abs();
    }
    
    class AbsFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			if (actualParams[0].isInteger()) {
				stack.push(abs(stack.popInt()));
				return;
			} 
			
			if (actualParams[0].isLong()) {
				stack.push(abs(stack.popLong()));
				return;
			} 
			
			if (actualParams[0].isDouble()) {
				stack.push(abs(stack.popDouble()));
				return;
			} 
			
			if (actualParams[0].isDecimal()) {
				stack.push(abs(stack.popDecimal()));
				return;
			}
		} 

    }

    @TLFunctionAnnotation("Computes bitwise OR of two operands.")
    public static final Long bit_or(Long i, Long j) {
    	return i | j;
    }

    @TLFunctionAnnotation("Computes bitwise OR of two operands.")
    public static final Integer bit_or(Integer i, Integer j) {
    	return i | j;
    }
    
    class BitOrFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			if (actualParams[0].isInteger() && actualParams[1].isInteger()) {
				stack.push(bit_or(stack.popInt(), stack.popInt()));
				return;
			} 
			
			if (actualParams[0].isLong() && actualParams[1].isLong()) {
				stack.push(bit_or(stack.popLong(), stack.popLong()));
				return;
			} 
			
		} 

    }

    @TLFunctionAnnotation("Computes bitwise AND of two operands.")
    public static final Long bit_and(Long i, Long j) {
    	return i & j;
    }

    @TLFunctionAnnotation("Computes bitwise AND of two operands.")
    public static final Integer bit_and(Integer i, Integer j) {
    	return i & j;
    }
    
    class BitAndFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			if (actualParams[0].isInteger() && actualParams[1].isInteger()) {
				stack.push(bit_and(stack.popInt(), stack.popInt()));
				return;
			} 
			
			if (actualParams[0].isLong() && actualParams[1].isLong()) {
				stack.push(bit_and(stack.popLong(), stack.popLong()));
				return;
			} 
			
		} 

    }

    @TLFunctionAnnotation("Computes bitwise AND of two operands.")
    public static final Long bit_xor(Long i, Long j) {
    	return i ^ j;
    }

    @TLFunctionAnnotation("Computes bitwise AND of two operands.")
    public static final Integer bit_xor(Integer i, Integer j) {
    	return i ^ j;
    }
    
    class BitXorFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			if (actualParams[0].isInteger() && actualParams[1].isInteger()) {
				stack.push(bit_xor(stack.popInt(), stack.popInt()));
				return;
			} 
			
			if (actualParams[0].isLong() && actualParams[1].isLong()) {
				stack.push(bit_xor(stack.popLong(), stack.popLong()));
				return;
			} 
		} 
    }
    
    @TLFunctionAnnotation("Shifts the first operand to the left by bits specified in the second operand.")
    public static final Long bit_lshift(Long j, Long i) {
    	return i << j;
    }

    @TLFunctionAnnotation("Shifts the first operand to the left by bits specified in the second operand.")
    public static final Integer bit_lshift(Integer j, Integer i) {
    	return i << j;
    }
    
    class BitLShiftFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			if (actualParams[0].isInteger() && actualParams[1].isInteger()) {
				stack.push(bit_lshift(stack.popInt(), stack.popInt()));
				return;
			} 
			
			if (actualParams[0].isLong() && actualParams[1].isLong()) {
				stack.push(bit_lshift(stack.popLong(), stack.popLong()));
				return;
			} 
		} 
    }

    @TLFunctionAnnotation("Shifts the first operand to the right by bits specified in the second operand.")
    public static final Long bit_rshift(Long j, Long i) {
    	return i >> j;
    }

    @TLFunctionAnnotation("Shifts the first operand to the right by bits specified in the second operand.")
    public static final Integer bit_rshift(Integer j, Integer i) {
    	return i >> j;
    }
    
    class BitRShiftFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			if (actualParams[0].isInteger() && actualParams[1].isInteger()) {
				stack.push(bit_rshift(stack.popInt(), stack.popInt()));
				return;
			} 
			
			if (actualParams[0].isLong() && actualParams[1].isLong()) {
				stack.push(bit_rshift(stack.popLong(), stack.popLong()));
				return;
			} 
		} 
    }
    
        
    @TLFunctionAnnotation("Inverts all bits in argument")
    public static final Long bit_negate(Long i) {
    	return ~i;
    }
    
    @TLFunctionAnnotation("Inverts all bits in argument")
    public static final Integer bit_negate(Integer i) {
    	return ~i;
    }
    
    class BitNegateFunction implements TLFunctionPrototype {
    	
    	public void execute(Stack stack, TLType[] actualParams) {
    		if (actualParams[0].isInteger()) {
    			stack.push(bit_negate(stack.popInt()));
    			return;
    		}
    		if (actualParams[0].isLong()) {
    			stack.push(bit_negate(stack.popLong()));
    			return;
    		}
    	}
    	
    }
    
//    @TLFunctionAnnotation("Sets or resets n-th bit of 1st argument")
    
    @TLFunctionAnnotation("Random Gaussian number.")
    public static final Double random_gaussian() {
    	return getGenerator(Thread.currentThread()).nextGaussian();
    }
    
    @TLFunctionAnnotation("Random Gaussian number. Allows changing seed.")
    public static final Double random_gaussian(Long seed) {
    	DataGenerator generator = getGenerator(Thread.currentThread());
    	generator.setSeed(seed);
    	return generator.nextGaussian();
    }
    
    // RANDOM Gaussian
    class RandomGaussianFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			if (actualParams.length == 1) {
				stack.push(random_gaussian(stack.popLong()));
			} else {
				stack.push(random_gaussian());
			}
		}
    }
    
    @TLFunctionAnnotation("Random boolean.")
    public static final Boolean random_boolean() {
    	return getGenerator(Thread.currentThread()).nextBoolean();
    }
    
    @TLFunctionAnnotation("Random boolean. Allows changing seed.")
    public static final Boolean random_boolean(Long seed) {
    	DataGenerator generator = getGenerator(Thread.currentThread());
    	generator.setSeed(seed);
    	return generator.nextBoolean();
    }
    // RANDOM Boolean
    class RandomBooleanFunction implements TLFunctionPrototype {
        
		public void execute(Stack stack, TLType[] actualParams) {
			if (actualParams.length == 1) {
				stack.push(random_boolean(stack.popLong()));
			} else {
				stack.push(random_boolean());
			}
		}
    }
    
    @TLFunctionAnnotation("Random integer.")
    public static final Integer random_int() {
    	return getGenerator(Thread.currentThread()).nextInt();
    }
    
    @TLFunctionAnnotation("Random integer. Allows changing seed.")
    public static final Integer random_int(Long seed) {
    	DataGenerator generator = getGenerator(Thread.currentThread());
    	generator.setSeed(seed);
    	return generator.nextInt();
    }
    
    @TLFunctionAnnotation("Random integer. Allows changing start and end value.")
    public static final Integer random_int(Integer min, Integer max) {
    	return getGenerator(Thread.currentThread()).nextInt(min, max);
    }
    
    @TLFunctionAnnotation("Random integer. Allows changing start, end value and seed.")
    public static final Integer random_int(Integer min, Integer max, Long seed) {
    	DataGenerator generator = getGenerator(Thread.currentThread());
    	generator.setSeed(seed);
    	return generator.nextInt(min, max);
    }
    
    // RANDOM_INT
    class RandomIntFunction implements TLFunctionPrototype {
    	
		public void execute(Stack stack, TLType[] actualParams) {
			Long seed;
			Integer max;
			Integer min;
			switch (actualParams.length) {
				case 3:
					seed = stack.popLong();
					max = stack.popInt();
					min = stack.popInt();
					stack.push(random_int(min, max, seed));
					break;
				case 2:
					max = stack.popInt();
					min = stack.popInt();
					stack.push(random_int(min, max));
					break;
				case 1:
					seed = stack.popLong();
					stack.push(random_int(seed));
					break;
				case 0:
					stack.push(random_int());
					break;
			}
		}
    }

    @TLFunctionAnnotation("Random long.")
    public static final Long random_long() {
    	return getGenerator(Thread.currentThread()).nextLong();
    }
    
    @TLFunctionAnnotation("Random long. Allows changing seed.")
    public static final Long random_long(Long seed) {
    	DataGenerator generator = getGenerator(Thread.currentThread());
    	generator.setSeed(seed);
    	return generator.nextLong();
    }
    
    @TLFunctionAnnotation("Random long. Allows changing start and end value.")
    public static final Long random_long(Long min, Long max) {
    	return getGenerator(Thread.currentThread()).nextLong(min, max);
    }
    
    @TLFunctionAnnotation("Random long. Allows changing start, end value and seed.")
    public static final Long random_long(Long min, Long max, Long seed) {
    	DataGenerator generator = getGenerator(Thread.currentThread());
    	generator.setSeed(seed);
    	return generator.nextLong(min, max);
    }
    
    // RANDOM_LONG
    class RandomLongFunction implements TLFunctionPrototype {
        
		public void execute(Stack stack, TLType[] actualParams) {
			Long seed;
			Long max;
			Long min;
			switch (actualParams.length) {
				case 3:
					seed = stack.popLong();
					max = stack.popLong();
					min = stack.popLong();
					stack.push(random_long(min, max, seed));
					break;
				case 2:
					max = stack.popLong();
					min = stack.popLong();
					stack.push(random_long(min, max));
					break;
				case 1:
					seed = stack.popLong();
					stack.push(random_long(seed));
					break;
				case 0:
					stack.push(random_long());
					break;
			}
		}
    }
}
