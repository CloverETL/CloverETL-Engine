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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.jetel.ctl.Stack;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.util.DataGenerator;
import org.jetel.util.MiscUtils;
import org.jetel.util.formatter.DateFormatter;
import org.jetel.util.formatter.DateFormatterFactory;

/**
 * @author javlin (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jun 2, 2010
 */
public class RandomLib extends TLFunctionLibrary {
	
	private static Map<Object, DataGenerator> dataGenerators = new HashMap<Object, DataGenerator>();
	
	private static synchronized DataGenerator getGenerator(Object key) {
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
			"random".equals(functionName) ? new RandomFunction() :
			"randomGaussian".equals(functionName) ? new RandomGaussianFunction() :
		    "randomBoolean".equals(functionName) ? new RandomBooleanFunction() : 
		    "randomInteger".equals(functionName) ? new RandomIntegerFunction() :
		    "randomLong".equals(functionName) ? new RandomLongFunction() :
			"randomString".equals(functionName) ? new RandomStringFunction() :
			"randomDate".equals(functionName) ? new RandomDateFunction() :
			"setRandomSeed".equals(functionName) ? new SetRandomSeedFunction() : null;
			
		if (ret == null) {
			throw new IllegalArgumentException("Unknown function '" + functionName + "'");
		}

		return ret;
	}
	
	private static String LIBRARY_NAME = "Random";

	public String getName() {
		return LIBRARY_NAME;
	}


	@TLFunctionAnnotation("Random number (>=0, <1)")
	public static final Double random(TLFunctionCallContext context) {
		return getGenerator(context.getTransformationID()).nextDouble();
	}

	// RANDOM
	class RandomFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(random(context));
		}
	}

	@TLFunctionAnnotation("Random Gaussian number.")
	public static final Double randomGaussian(TLFunctionCallContext context) {
		return getGenerator(context.getTransformationID()).nextGaussian();
	}

	// RANDOM Gaussian
	class RandomGaussianFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(randomGaussian(context));
		}
	}

	@TLFunctionAnnotation("Random boolean.")
	public static final Boolean randomBoolean(TLFunctionCallContext context) {
		return getGenerator(context.getTransformationID()).nextBoolean();
	}

	// RANDOM Boolean
	class RandomBooleanFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(randomBoolean(context));
		}
	}

	@TLFunctionAnnotation("Random integer.")
	public static final Integer randomInteger(TLFunctionCallContext context) {
		return getGenerator(context.getTransformationID()).nextInt();
	}

	@TLFunctionAnnotation("Random integer. Allows changing start and end value.")
	public static final Integer randomInteger(TLFunctionCallContext context, Integer min, Integer max) {
		return getGenerator(context.getTransformationID()).nextInt(min, max);
	}

	// RANDOMINTEGER
	class RandomIntegerFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			Integer max;
			Integer min;
			if (context.getParams().length == 2) {
				max = stack.popInt();
				min = stack.popInt();
				stack.push(randomInteger(context, min, max));
			} else {
				stack.push(randomInteger(context));
			}
		}
	}

	@TLFunctionAnnotation("Random long.")
	public static final Long randomLong(TLFunctionCallContext context) {
		return getGenerator(context.getTransformationID()).nextLong();
	}

	@TLFunctionAnnotation("Random long. Allows changing start and end value.")
	public static final Long randomLong(TLFunctionCallContext context, Long min, Long max) {
		return getGenerator(context.getTransformationID()).nextLong(min, max);
	}

	// RANDOMLONG
	class RandomLongFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			Long max;
			Long min;
			if (context.getParams().length == 2) {
				max = stack.popLong();
				min = stack.popLong();
				stack.push(randomLong(context, min, max));
			} else {
				stack.push(randomLong(context));
			}
		}
	}

	@TLFunctionAnnotation("Generates a random string.")
	public static String randomString(TLFunctionCallContext context, int minLength, int maxLength) {
		return getGenerator(context.getTransformationID()).nextString(minLength, maxLength);
	}

	class RandomStringFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			Integer maxLength = stack.popInt();
			Integer minLength = stack.popInt();
			stack.push(randomString(context, minLength, maxLength));
		}
	}

	// Random date
	class RandomDateFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
			randomDateInit(context);
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			String locale = null;
			String format;
			if (context.getParams().length > 3) {
				locale = stack.popString();
				format = stack.popString();
				String to = stack.popString();
				String from = stack.popString();
				stack.push(randomDate(context, from, to, format, locale));
			} else if (context.getParams().length > 2) {
				format = stack.popString();
				String to = stack.popString();
				String from = stack.popString();
				stack.push(randomDate(context, from, to, format));
			} else {
				if (context.getParams()[1].isDate()) {
					Date to = stack.popDate();
					Date from = stack.popDate();
					stack.push(randomDate(context, from, to));
				} else {
					Long to = stack.popLong();
					Long from = stack.popLong();
					stack.push(randomDate(context, from, to));
				}
			}
		}
	}

	@TLFunctionInitAnnotation
	public static final void randomDateInit(TLFunctionCallContext context) {
		context.setCache(new TLDateFormatLocaleCache(context, 2, 3));
	}

	@TLFunctionAnnotation("Generates a random date from interval specified by two dates.")
	public static final Date randomDate(TLFunctionCallContext context, Date from, Date to) {
		return randomDate(context, from.getTime(), to.getTime());
	}

	@TLFunctionAnnotation("Generates a random date from interval specified by Long representation of dates.")
	public static final Date randomDate(TLFunctionCallContext context, Long from, Long to) {
		if (from > to) {
			throw new TransformLangExecutorRuntimeException("randomDate - fromDate is greater than toDate");
		}
		return new Date(getGenerator(context.getTransformationID()).nextLong(from, to));
	}

	@TLFunctionAnnotation("Generates a random date from interval specified by string representation of dates in given format.")
	public static final Date randomDate(TLFunctionCallContext context, String from, String to, String format) {
		DateFormatter df = ((TLDateFormatLocaleCache)context.getCache()).getCachedLocaleFormatter(context, format, null, 1, 2);
		return randomDate(context, from, to, df);
	}

	@TLFunctionAnnotation("Generates a random from interval specified by string representation of dates in given format and locale.")
	public static final Date randomDate(TLFunctionCallContext context, String from, String to, String format,
			String locale) {
		DateFormatter df = DateFormatterFactory.getFormatter(format, MiscUtils.createLocale(locale));
		return randomDate(context, from, to, df);
	}

	private static final Date randomDate(TLFunctionCallContext context, String from, String to, DateFormatter formatter) {
		try {
			long fromTime = formatter.parseMillis(from);
			long toTime = formatter.parseMillis(to);
			return randomDate(context, fromTime, toTime);
		} catch (IllegalArgumentException e) {
			throw new TransformLangExecutorRuntimeException("randomDate - " + e.getMessage());
		}
	}
	
	@TLFunctionAnnotation("Changes seed of random.")
	public static void setRandomSeed(TLFunctionCallContext context, long randomSeed) {
		DataGenerator generator = getGenerator(context.getTransformationID());
		generator.setSeed(randomSeed);
	}

	class SetRandomSeedFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			setRandomSeed(context, stack.popLong());
		}
	}
}
