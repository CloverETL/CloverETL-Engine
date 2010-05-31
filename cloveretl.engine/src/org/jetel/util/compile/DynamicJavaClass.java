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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.ComponentNotReadyException;

/**
 * Utility class for dynamic compiling of Java source code. Offers instantiating of compiled code.
 */
public final class DynamicJavaClass {

	private static final Log logger = LogFactory.getLog(DynamicJavaClass.class);

	private static final Pattern PACKAGE_PATTERN = Pattern.compile("package\\s+([^;]+);");
	private static final Pattern CLASS_PATTERN = Pattern.compile("class\\s+(\\w+)");

	public static Object instantiate(String sourceCode, ClassLoader classLoader) throws ComponentNotReadyException {
		DynamicCompiler compiler = new DynamicCompiler(classLoader);
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

	private static String extractClassName(String sourceCode) {
		Matcher classMatcher = CLASS_PATTERN.matcher(sourceCode);

		if (!classMatcher.find()) {
			throw new IllegalArgumentException("Cannot find class name within sourceCode!");
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
