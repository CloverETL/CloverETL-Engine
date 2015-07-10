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

import org.jetel.ctl.Stack;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.util.DataGenerator;
import org.jetel.util.formatter.DateFormatter;

/**
 * @author javlin (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jun 2, 2010
 */
public class RandomLib extends TLFunctionLibrary {
	
	private static final String DATA_GENERATOR_KEY = "DATA_GENERATOR";
	
	/*
	 * CLO-722
	 */
	private static DataGenerator getGenerator(TLFunctionCallContext context) {
		TLTransformationContext c = context.getTransformationContext();
		// TLTransformationContext should be used by a single thread => no synchronization needed
		DataGenerator generator = (DataGenerator) c.getCachedObject(DATA_GENERATOR_KEY);
		if (generator == null) {
			generator = new DataGenerator();
			c.setCachedObject(DATA_GENERATOR_KEY, generator);
		}
		return generator;
    }

	@Override
	public TLFunctionPrototype getExecutable(String functionName) {
		if (functionName != null) {
			switch (functionName) {
				case "random": return new RandomFunction();
				case "randomGaussian": return new RandomGaussianFunction();
				case "randomBoolean": return new RandomBooleanFunction();
				case "randomInteger": return new RandomIntegerFunction();
				case "randomLong": return new RandomLongFunction();
				case "randomString": return new RandomStringFunction();
				case "randomDate": return new RandomDateFunction();
				case "setRandomSeed": return new SetRandomSeedFunction();
			}
		}
			
		throw new IllegalArgumentException("Unknown function '" + functionName + "'");
	}
	
	private static String LIBRARY_NAME = "Random";

	@Override
	public String getName() {
		return LIBRARY_NAME;
	}

	@TLFunctionInitAnnotation
    public static final void randomInit(TLFunctionCallContext context) {
    	context.setCache(new TLDataGeneratorCache(getGenerator(context)));
    }	

	@TLFunctionAnnotation("Random number (>=0, <1)")
	public static final Double random(TLFunctionCallContext context) {
		return ((TLDataGeneratorCache) context.getCache()).getDataGenerator().nextDouble();
	}

	// RANDOM
	static class RandomFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			randomInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(random(context));
		}
	}

	@TLFunctionInitAnnotation
    public static final void randomGaussianInit(TLFunctionCallContext context) {
    	context.setCache(new TLDataGeneratorCache(getGenerator(context)));
    }	

	@TLFunctionAnnotation("Random Gaussian number.")
	public static final Double randomGaussian(TLFunctionCallContext context) {
		return ((TLDataGeneratorCache) context.getCache()).getDataGenerator().nextGaussian();
	}

	// RANDOM Gaussian
	static class RandomGaussianFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			randomGaussianInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(randomGaussian(context));
		}
	}

	@TLFunctionInitAnnotation
    public static final void randomBooleanInit(TLFunctionCallContext context) {
    	context.setCache(new TLDataGeneratorCache(getGenerator(context)));
    }	

	@TLFunctionAnnotation("Random boolean.")
	public static final Boolean randomBoolean(TLFunctionCallContext context) {
		return ((TLDataGeneratorCache) context.getCache()).getDataGenerator().nextBoolean();
	}

	// RANDOM Boolean
	static class RandomBooleanFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			randomBooleanInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(randomBoolean(context));
		}
	}

	@TLFunctionInitAnnotation
    public static final void randomIntegerInit(TLFunctionCallContext context) {
    	context.setCache(new TLDataGeneratorCache(getGenerator(context)));
    }	

	@TLFunctionAnnotation("Random integer.")
	public static final Integer randomInteger(TLFunctionCallContext context) {
		return ((TLDataGeneratorCache) context.getCache()).getDataGenerator().nextInt();
	}

	@TLFunctionAnnotation("Random integer. Allows changing start and end value.")
	public static final Integer randomInteger(TLFunctionCallContext context, Integer min, Integer max) {
		return ((TLDataGeneratorCache) context.getCache()).getDataGenerator().nextInt(min, max);
	}

	// RANDOMINTEGER
	static class RandomIntegerFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			randomIntegerInit(context);
		}

		@Override
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

	@TLFunctionInitAnnotation
    public static final void randomLongInit(TLFunctionCallContext context) {
    	context.setCache(new TLDataGeneratorCache(getGenerator(context)));
    }	

	@TLFunctionAnnotation("Random long.")
	public static final Long randomLong(TLFunctionCallContext context) {
		return ((TLDataGeneratorCache) context.getCache()).getDataGenerator().nextLong();
	}

	@TLFunctionAnnotation("Random long. Allows changing start and end value.")
	public static final Long randomLong(TLFunctionCallContext context, Long min, Long max) {
		return ((TLDataGeneratorCache) context.getCache()).getDataGenerator().nextLong(min, max);
	}

	// RANDOMLONG
	static class RandomLongFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			randomLongInit(context);
		}

		@Override
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

	@TLFunctionInitAnnotation
    public static final void randomStringInit(TLFunctionCallContext context) {
    	context.setCache(new TLDataGeneratorCache(getGenerator(context)));
    }	

	@TLFunctionAnnotation("Generates a random string.")
	public static String randomString(TLFunctionCallContext context, Integer minLength, Integer maxLength) {
		return ((TLDataGeneratorCache) context.getCache()).getDataGenerator().nextString(minLength, maxLength);
	}

	static class RandomStringFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			randomStringInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			Integer maxLength = stack.popInt();
			Integer minLength = stack.popInt();
			stack.push(randomString(context, minLength, maxLength));
		}
	}

	// Random date
	static class RandomDateFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			randomDateInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			String locale = null;
			String timeZone = null;
			String format;
			if (context.getParams().length > 4) {
				timeZone = stack.popString();
				locale = stack.popString();
				format = stack.popString();
				String to = stack.popString();
				String from = stack.popString();
				stack.push(randomDate(context, from, to, format, locale, timeZone));
			} else if (context.getParams().length > 3) {
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
		context.setCache(new TLDateFormatLocaleCache(context, 2, 3, 4));
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
		return new Date(getGenerator(context).nextLong(from, to));
	}

	@TLFunctionAnnotation("Generates a random date from interval specified by string representation of dates in given format.")
	public static final Date randomDate(TLFunctionCallContext context, String from, String to, String format) {
		return randomDate(context, from, to, format, null);
	}

	@TLFunctionAnnotation("Generates a random from interval specified by string representation of dates in given format and locale.")
	public static final Date randomDate(TLFunctionCallContext context, String from, String to, String format, String locale) {
		return randomDate(context, from, to, format, locale, null);
	}

	@TLFunctionAnnotation("Generates a random from interval specified by string representation of dates in given format, locale and time zone.")
	public static final Date randomDate(TLFunctionCallContext context, String from, String to, String format, String locale, String timeZone) {
		DateFormatter df = ((TLDateFormatLocaleCache) context.getCache()).getCachedLocaleFormatter(context, format, locale, timeZone, 2, 3, 4);
		return randomDate(context, from, to, df);
	}

	private static final Date randomDate(TLFunctionCallContext context, String from, String to, DateFormatter formatter) {
		try {
			long fromTime = formatter.parseMillis(from);
			long toTime = formatter.parseMillis(to);
			return randomDate(context, fromTime, toTime);
		} catch (IllegalArgumentException e) {
			throw new TransformLangExecutorRuntimeException("randomDate", e);
		}
	}

	@TLFunctionInitAnnotation
    public static final void setRandomSeedInit(TLFunctionCallContext context) {
    	context.setCache(new TLDataGeneratorCache(getGenerator(context)));
    }	

	@TLFunctionAnnotation("Changes seed of random.")
	public static void setRandomSeed(TLFunctionCallContext context, Long randomSeed) {
		((TLDataGeneratorCache) context.getCache()).getDataGenerator().setSeed(randomSeed);
	}

	static class SetRandomSeedFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			setRandomSeedInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			setRandomSeed(context, stack.popLong());
		}
	}
}
