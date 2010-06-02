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
package org.jetel.util.compile;

import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.ComponentNotReadyException;

/**
 * Utility class for dynamic compiling of Java source code. Offers instantiating of compiled code.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 1st June 2010
 * @since 31st May 2010
 */
public final class DynamicJavaClass {

	/** Logger for this utility class. */
	private static final Log logger = LogFactory.getLog(DynamicJavaClass.class);

	/** Regex pattern used to extract package of the class to be compiled. */
	private static final Pattern PACKAGE_PATTERN = Pattern.compile("package\\s+([^;]+);");
	/** Regex pattern used to extract class to be compiled. */
	private static final Pattern CLASS_PATTERN = Pattern.compile("class\\s+(\\w+)");

	/**
	 * Instantiates a class present within the given Java source code.
	 *
	 * @param sourceCode the Java source code with a class to be instantiated
	 * @param classLoader the class loader to be used, may be <code>null</code>
	 * @param classPathUrls the array of additional class path URLs, may be <code>null</code>
	 *
	 * @return instance of the class present within the source code
	 *
	 * @throws ComponentNotReadyException if instantiation of the class failed for some reason
	 */
	public static Object instantiate(String sourceCode, ClassLoader classLoader, URL[] classPathUrls)
			throws ComponentNotReadyException {
		DynamicCompiler compiler = new DynamicCompiler(classLoader, classPathUrls);
		String className = extractClassName(sourceCode);

		logger.info("Compiling dynamic class " + className + "...");

		try {
			Object result = compiler.compile(sourceCode, className).newInstance();
			logger.info("Dynamic class " + className + " successfully compiled and instantiated.");

			return result;
		} catch (CompilationException exception) {
        	logger.debug(exception.getCompilerOutput());
        	logger.debug(sourceCode);

        	throw new ComponentNotReadyException("Cannot compile the dynamic class!", exception);
		} catch (IllegalAccessException exception) {
            throw new ComponentNotReadyException("Cannot access the dynamic class!", exception);
        } catch (InstantiationException exception) {
            throw new ComponentNotReadyException("Cannot instantiate the dynamic class!", exception);
		}
	}

	private static String extractClassName(String sourceCode) throws ComponentNotReadyException {
		Matcher classMatcher = CLASS_PATTERN.matcher(sourceCode);

		if (!classMatcher.find()) {
			throw new ComponentNotReadyException("Cannot find class name within sourceCode!");
		}

		Matcher packageMatcher = PACKAGE_PATTERN.matcher(sourceCode);

		if (packageMatcher.find()) {
			return packageMatcher.group(1) + "." + classMatcher.group(1);
		}

		return classMatcher.group(1);
	}

	private DynamicJavaClass() {
		throw new UnsupportedOperationException();
	}

}
