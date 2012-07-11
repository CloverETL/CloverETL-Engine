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
package org.jetel.data.parser;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author csochor (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Dec 6, 2010
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public final class TextParserFactory {
	private final static Log logger = LogFactory.getLog(TextParserFactory.class);

	private static final Class[] availableParsers = new Class[] { SimpleDataParser.class, DataParser.class, CharByteDataParser.class };

	private TextParserFactory() {
	}
	
	/**
	 * Creates data parser according to the configuration and data type. The fastes parser implementation is used.
	 * 
	 * Each {@link TextParser} implementation must have method <code>getParserSpeed</code>. This method returns null
	 * value if parser can't parse specified configuration, otherwise return parser speed as integer
	 * <code>0(slowest)-100(fastest)</code>.
	 * 
	 * @param cfg
	 *            requested parser configuration
	 * @param parserClassName
	 *            parser class name
	 * @param parserClassLoader
	 *            Class loader of the parser. Can be null (in this case current class loader is used).
	 * @return fastest parse implemementation, always is not null
	 * @throws JetelRuntimeException
	 *             if no parser found
	 */
	public static final TextParser getParser(TextParserConfiguration cfg, String parserClassName, ClassLoader parserClassLoader) {
		Class bestParserClass = null;
		if (parserClassName != null) {
			try {
				if (parserClassLoader != null) {
					bestParserClass = Class.forName(parserClassName, true, parserClassLoader);
				} else {
					bestParserClass = Class.forName(parserClassName);
				}
			} catch (ClassNotFoundException e) {
				throw new JetelRuntimeException("Invalid parser '" + parserClassName + "'", e);
			}
		} else {
			Integer bestSpeed = null;
			for (Class parserClass : availableParsers) {
				try {
					final Method speedMethod = parserClass.getMethod("getParserSpeed", TextParserConfiguration.class);
					final Integer parserSpeed = (Integer) speedMethod.invoke(null, cfg);
					if (logger.isDebugEnabled()) {
						logger.debug("Speed " + parserSpeed + " for parser " + parserClass.getName() + " and configuration " + cfg);
					}
					if (parserSpeed != null) {
						if (bestSpeed == null || bestSpeed < parserSpeed) {
							bestSpeed = parserSpeed;
							bestParserClass = parserClass;
						}
					}
				} catch (Throwable e) {
					logger.error("Invalid parser " + parserClass.getName(), e);
				}
			}
			if (bestSpeed == null) {
				throw new JetelRuntimeException("No parser can parser " + cfg + " available parsers: " + Arrays.asList(availableParsers));
			}
		}

		try {
			final Constructor maxParserConstructor = bestParserClass.getConstructor(TextParserConfiguration.class);
			final TextParser ret = (TextParser) maxParserConstructor.newInstance(cfg);
			return ret;
		} catch (Throwable e) {
			throw new JetelRuntimeException("Invalid parser " + bestParserClass.getName(), e);
		}
	}

	public static final TextParser getParser(TextParserConfiguration cfg, String parserClassName){
		return getParser(cfg, parserClassName, null);
	}
	
	public static final TextParser getParser(TextParserConfiguration cfg) {
		return getParser(cfg, null);
	}

	public static final TextParser getParser(DataRecordMetadata metadata) {
		return getParser(new TextParserConfiguration(metadata));
	}

	public static final TextParser getParser(DataRecordMetadata metadata, String charset) {
		return getParser(new TextParserConfiguration(metadata, charset));
	}

	public static final TextParser getParser(DataRecordMetadata metadata, String charset, boolean verbose) {
		return getParser(new TextParserConfiguration(metadata, charset, verbose));
	}

}
