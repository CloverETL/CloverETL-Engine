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
import org.jetel.exception.LoadClassException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.Node;

/**
 * Utility class for dynamic compiling of Java source code. Offers instantiating of compiled code.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 2nd June 2010
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
	 * Instantiates class written in given source code.
	 * @param sourceCode
	 * @return
	 * @throws LoadClassException if instantiation of loaded code fails
	 */
	public static Object instantiate(String sourceCode) {
		Node node = ContextProvider.getNode();
		if (node != null) {
			return instantiate(sourceCode, node);
		}
		return instantiate(sourceCode, Thread.currentThread().getContextClassLoader());
	}
	
	/**
	 * Instantiates class written in given source code.
	 * The supplied node is the context (class loader, class path).
	 *  
	 * @param sourceCode
	 * @param node
	 * @return
	 * @throws LoadClassException if instantiation of loaded code fails
	 */
	public static Object instantiate(String sourceCode, Node node) {
		return instantiate(sourceCode, node.getClass().getClassLoader(),
				node.getGraph().getRuntimeContext().getCompileClassPath());
	}
	
	/**
	 * Instantiates class written in given source code and attempts cast it to given type.
	 * @param <T>
	 * @param sourceCode
	 * @param expectedType
	 * @param node
	 * @return
	 * @throws LoadClassException if instantiation of loaded code fails
	 */
	public static <T> T instantiate(String sourceCode, Class<T> expectedType, Node node) {
		Object instance = instantiate(sourceCode, node);
		try {
			return expectedType.cast(instance);
		} catch (ClassCastException e) {
			throw new LoadClassException("Provided class does not extend/implement " + expectedType.getName(), e);
		}
	}
	
	/**
	 * Instantiates a class present within the given Java source code.
	 *
	 * @param sourceCode the Java source code with a class to be instantiated
	 * @param classLoader the class loader to be used, may be <code>null</code>
	 * @param classPathUrls the array of additional class path URLs, may be <code>null</code>
	 *
	 * @return instance of the class present within the source code
	 *
	 * @throws LoadClassException if instantiation of the class failed for some reason
	 */
	public static Object instantiate(String sourceCode, ClassLoader classLoader, URL ... compileClassPath) {
		DynamicCompiler compiler = new DynamicCompiler(classLoader, compileClassPath);
		String className = extractClassName(sourceCode);

		logger.info("Compiling dynamic class " + className + "...");

		try {
			Object result = compiler.compile(sourceCode, className).newInstance();
			logger.info("Dynamic class " + className + " successfully compiled and instantiated.");

			return result;
		} catch (CompilationException e) {
        	logger.error("Compiler output:\n" + e.getCompilerOutput());
        	logger.debug("Source code:\n" + sourceCode);

        	throw new LoadClassException("Cannot compile the dynamic class!", e);
		} catch (IllegalAccessException e) {
            throw new LoadClassException("Cannot access the dynamic class!", e);
        } catch (InstantiationException e) {
            throw new LoadClassException("Cannot instantiate the dynamic class!", e);
		}
	}

	private static String extractClassName(String sourceCode) {
		Matcher classMatcher = CLASS_PATTERN.matcher(sourceCode);

		if (!classMatcher.find()) {
			throw new LoadClassException("Cannot find class name within sourceCode!");
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
