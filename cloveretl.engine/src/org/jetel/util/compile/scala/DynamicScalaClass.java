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
package org.jetel.util.compile.scala;

import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.JetelRuntimeException;

/**
 * Utility class for dynamic compiling of Scala source code. Offers instantiating of compiled code.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 22 Dec 2011
 */
public final class DynamicScalaClass {

	/** Logger for this utility class. */
	private static final Log logger = LogFactory.getLog(DynamicScalaClass.class);

	/** Regex pattern used to extract package of the class to be compiled. */
	private static final Pattern PACKAGE_PATTERN = Pattern.compile("package\\s+([^;]+)");
	/** Regex pattern used to extract class to be compiled. */
	private static final Pattern CLASS_PATTERN = Pattern.compile("class\\s+(\\w+)");

	/**
	 * Instantiates a class present within the given Scala source code.
	 *
	 * @param sourceCode the Scala source code with a class to be instantiated
	 * @param classLoader the class loader to be used, may be <code>null</code>
	 * @param compileClassPath the array of additional class path URLs, may be <code>null</code>
	 *
	 * @return instance of the class present within the source code
	 */
	public static Object instantiate(String sourceCode, ClassLoader classLoader, URL... compileClassPath) {
		//TODO compileClassPath are ignored now
		DynamicScalaCompiler compiler = new DynamicScalaCompiler(classLoader);
		String className = extractClassName(sourceCode);

		logger.info("Compiling dynamic scala class " + className + "...");

		try {
			Object result = compiler.compile(sourceCode, className).newInstance();
			logger.info("Dynamic class " + className + " successfully compiled and instantiated.");

			return result;
		} catch (Exception e) {
			throw new JetelRuntimeException("Scala class compilation failed.", e);
		}
	}
	
	private static String extractClassName(String sourceCode) {
		Matcher classMatcher = CLASS_PATTERN.matcher(sourceCode);

		if (!classMatcher.find()) {
			throw new JetelRuntimeException("Cannot find class name within sourceCode!");
		}

		Matcher packageMatcher = PACKAGE_PATTERN.matcher(sourceCode);

		if (packageMatcher.find()) {
			return packageMatcher.group(1) + "." + classMatcher.group(1);
		}

		return classMatcher.group(1);
	}

	private DynamicScalaClass() {
		throw new UnsupportedOperationException();
	}

}
