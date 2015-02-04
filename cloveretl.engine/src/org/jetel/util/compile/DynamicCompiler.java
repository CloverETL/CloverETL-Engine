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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.ctl.TLCompilerDescription;
import org.jetel.ctl.extensions.TLFunctionLibraryDescription;
import org.jetel.data.Defaults;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.plugin.PluginDescriptor;
import org.jetel.plugin.Plugins;

/**
 * Java compiler for dynamic compilation of Java source code.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 2nd June 2010
 * @created 14th May 2010
 *
 * @see JavaCompiler
 */
public final class DynamicCompiler {

	private static final Log logger = LogFactory.getLog(DynamicCompiler.class);

	/** The class loader to be used during compilation and class loading. */
	private final ClassLoader classLoader;
	/** Additional class path URLs to be used during compilation and class loading. */
	private final URL[] compileClassPath;

	/**
	 * Constructs a <code>DynamicCompiler</code> instance for a given class loader to be used during compilation.
	 * Additional class path URLs may be provided if any external Java classes are required.
	 *
	 * @param classLoader the class loader to load classes referenced from unit having been compiled
	 * @param compileClassPath the array of additional class path URLs, may be <code>null</code>
	 */
	public DynamicCompiler(ClassLoader classLoader, URL... compileClassPath) {
		this.classLoader = classLoader;
		this.compileClassPath = compileClassPath; // <- potential encapsulation problem, defensive copying would solve that
	}

	/**
	 * Compiles a given piece of Java source code and then loads a desired class. This method may be called repeatedly.
	 *
	 * @param sourceCode the Java source code to be compiled, may not be <code>null</code>
	 * @param className the name of a Java class (present in the source) code to be loaded, may not be <code>null</code>
	 *
	 * @return the class instance loaded from the compiled source code
	 *
	 * @throws NullPointerException if either the argument is <code>null</code>
	 * @throws CompilationException if the compilation failed
	 */
	public Class<?> compile(String sourceCode, String className) throws CompilationException {
		if (sourceCode == null) {
			throw new NullPointerException("sourceCode is null!");
		}

		if (className == null) {
			throw new NullPointerException("className is null!");
		}

		if (!Defaults.USE_DYNAMIC_COMPILER) {
			throw new JetelRuntimeException("Dynamic java compilation is not allowed (USE_DYNAMIC_COMPILER variable in DefaultProperties is false)");
		}
		
		final Set<URL> extraLibraries = getExtraLibraries();
		extraLibraries.addAll(Arrays.asList(compileClassPath));

		JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
		if (compiler == null)
			throw new IllegalStateException("Used Java Platform doesn't provide any java compiler! ");
		JavaClassFileManager fileManager = new JavaClassFileManager(compiler, classLoader, extraLibraries.toArray(new URL[extraLibraries.size()]));
		
		logger.debug("Java compile time classpath (-cp) for class '" + className + "': " + fileManager.getClassPath());
		
		StringWriter compilerOutput = new StringWriter();

		CompilationTask task = compiler.getTask(compilerOutput, fileManager, null,
				Arrays.asList("-cp", fileManager.getClassPath()), null,
				Arrays.asList(new JavaSourceFileObject(className, sourceCode)));

		if (!task.call()) {
			String outputString = compilerOutput.toString();
			String message = "Compilation failed! ";
			if (outputString != null) {
				try {
					message = outputString.substring(outputString.indexOf(" ") + 1, outputString.indexOf(System.getProperty("line.separator")));
					message += ". ";
				} catch (IndexOutOfBoundsException ex) {
				}
				message += "See log for more details";
			}
			throw new CompilationException(message , compilerOutput.toString());
		}

		try {
			return fileManager.loadClass(className);
		} catch (ClassNotFoundException exception) {
			throw new CompilationException("Loading of class " + className + " failed!", exception);
		}
	}

	/**
	 * This method returns in lazy way generated list of important libraries -
	 * - all referenced libraries in plugins which contains a 'ctlfunction', 'tlfunction' or 'tlCompiler'
	 * extension.
	 * @return set of libraries which should be part of compile time classpath
	 */
	public static Set<URL> getExtraLibraries() {
		
		final Set<URL> libraries = new HashSet<URL>();
		for (PluginDescriptor p : getCTLRelatedPlugins()) {
			libraries.addAll(Arrays.asList(p.getLibraryURLs()));
		}
		return libraries;
	}
	
	/**
	 * Answers plugins that declare extensions related to CTL (resp. TL).
	 * @return
	 */
	public static Collection<PluginDescriptor> getCTLRelatedPlugins() {
		
		final List<PluginDescriptor> result = new ArrayList<PluginDescriptor>();
		for (PluginDescriptor plugin : Plugins.getPluginDescriptors().values()) {
			if (!plugin.getExtensions(TLFunctionLibraryDescription.EXTENSION_POINT_ID).isEmpty() ||
				!plugin.getExtensions(org.jetel.interpreter.extensions.TLFunctionLibraryDescription.EXTENSION_POINT_ID).isEmpty() ||
				!plugin.getExtensions(TLCompilerDescription.EXTENSION_POINT_ID).isEmpty()) {
				result.add(plugin);
			}
		}
		return result;
	}

    /**
     * @return set of class loaders of plugins which contains CTL2 related code.
     * @see #getExtraLibraries()
     * @see #getCTLRelatedPlugins()
     */
    public static Set<ClassLoader> getCTLLibsClassLoaders() {
    	Set<ClassLoader> loaders = new LinkedHashSet<ClassLoader>();
    	for (PluginDescriptor plugin : DynamicCompiler.getCTLRelatedPlugins()) {
    		loaders.add(plugin.getClassLoader());
    	}
    	return loaders;
    }
    

	/**
	 * Java source code wrapper used by {@link JavaCompiler}.
	 *
	 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
	 *
	 * @version 28th May 2010
	 * @created 14th May 2010
	 */
	private static final class JavaSourceFileObject extends SimpleJavaFileObject {

		/** The Java source code. */
		private final String sourceCode;

		public JavaSourceFileObject(String name, String sourceCode) {
			super(URI.create(name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);

			this.sourceCode = sourceCode;
		}

		@Override
		public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
			return sourceCode;
		}

	}

	/**
	 * Java class file wrapper used by {@link JavaCompiler}.
	 *
	 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
	 *
	 * @version 28th May 2010
	 * @created 14th May 2010
	 */
	private static final class JavaClassFileObject extends SimpleJavaFileObject {

		/** The class data in a form of Java byte code. */
		private final ByteArrayOutputStream classData = new ByteArrayOutputStream();

		public JavaClassFileObject(String name) {
			super(URI.create(name.replace('.', '/') + Kind.CLASS.extension), Kind.CLASS);
		}

		@Override
		public OutputStream openOutputStream() throws IOException {
			return classData;
		}

		public byte[] getData() {
			return classData.toByteArray();
		}

	}

	/**
	 * Java class file manager used by {@link JavaCompiler}.
	 *
	 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
	 *
	 * @version 2nd June 2010
	 * @created 14th May 2010
	 */
	private static final class JavaClassFileManager extends ForwardingJavaFileManager<JavaFileManager> {

		/** The class loader used to load classes directly from Java byte code. */
		private final ByteCodeClassLoader classLoader;

		public JavaClassFileManager(JavaCompiler compiler, ClassLoader classLoader, URL[] classPathUrls) {
			super(compiler.getStandardFileManager(null, null, null));

			this.classLoader = new ByteCodeClassLoader(classLoader, classPathUrls);
		}

		public String getClassPath() {
			return ClassLoaderUtils.getClasspath(classLoader.getParent(), classLoader.getURLs());
		}

		@Override
		public JavaFileObject getJavaFileForOutput(Location location, String className, Kind kind, FileObject sibling)
				throws IOException {
			JavaClassFileObject javaClass = new JavaClassFileObject(className);
			classLoader.registerClass(className, javaClass);

			return javaClass;
		}

		public Class<?> loadClass(String name) throws ClassNotFoundException {
			return classLoader.loadClass(name);
		}

	}

	/**
	 * Class loader used to load classes directly from Java byte code.
	 *
	 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
	 *
	 * @version 2nd June 2010
	 * @created 14th May 2010
	 */
	private static final class ByteCodeClassLoader extends URLClassLoader {

		/** The map of compiled Java classes that can be loaded by this class loader. */
		private final Map<String, JavaClassFileObject> javaClasses = new HashMap<String, JavaClassFileObject>();

		public ByteCodeClassLoader(ClassLoader parent, URL[] classPathUrls) {
			super((classPathUrls != null) ? classPathUrls : new URL[0], parent);
		}

		public void registerClass(String name, JavaClassFileObject byteCode) {
			javaClasses.put(name, byteCode);
		}

		@Override
		protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
			Class<?> clazz = findLoadedClass(name);

			if (clazz == null) {
				JavaClassFileObject javaClass = javaClasses.get(name);

				if (javaClass != null) {
					try {
						byte[] classData = javaClass.getData();
						clazz = defineClass(name, classData, 0, classData.length);
					} catch(ClassFormatError error) {
						throw new ClassNotFoundException(name, error);
					}
				} else {
					clazz = super.loadClass(name, false);
				}
			}

			if (resolve) {
			    resolveClass(clazz);
			}

			return clazz;
		}

	}

}
