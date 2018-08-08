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

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.LoadClassException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.Node;
import org.jetel.graph.runtime.IAuthorityProxy;
import org.jetel.util.classloader.URLBasedClassLoader;
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.SandboxUrlUtils;
import org.jetel.util.string.UnicodeBlanks;

/**
 * @author David Pavlis, JavlinConsulting, s.r.o.
 * @author jan.michalica
 * @since 27.3.2006
 */
public class ClassLoaderUtils {

	private static final Log logger = LogFactory.getLog(ClassLoaderUtils.class);
	
	public static class SandboxURLException extends RuntimeException {
		
		private static final long serialVersionUID = 1L;
		
		public SandboxURLException(String message, Throwable cause) {
			super(message, cause);
		}
	}
	
	/**
	 * Builds platform-specific class path string from given class loader and provided URLs.
	 * It is presumed that the provided class loader is a URL-based class loader and its URLs are added to the result.
	 * In case it is a system class loader, JVM class path is included in the result.
	 * 
	 * @param loader
	 * @param includeEngineClasspath whether class path of the engine class loader should be included
	 * @param classPathUrls
	 * @return
	 */
	public static String getClasspath(ClassLoader loader, boolean includeEngineClasspath, URL ... classPathUrls) {
		
		Set<URL> urls = new LinkedHashSet<>();
		
		URL[] loaderClasspath = getClasspathUrls(loader);
		if (loaderClasspath != null) {
			urls.addAll(Arrays.asList(loaderClasspath));
		}
		
		if (classPathUrls != null) {
			urls.addAll(Arrays.asList(classPathUrls));
		}
		
		if (includeEngineClasspath) {
			URL[] engineClasspath = getClasspathUrls(ClassLoaderUtils.class.getClassLoader());
			if (engineClasspath != null) {
				urls.addAll(Arrays.asList(engineClasspath));
			}
		}
		
		StringBuilder result = new StringBuilder();
		
		for (Iterator<URL> it = urls.iterator(); it.hasNext();) {
			URL url = it.next();
			String fileName = getClasspathFilePath(url);
			if (!UnicodeBlanks.isBlank(fileName)) {
				result.append(fileName);
				if (it.hasNext()) {
					result.append(File.pathSeparator);
				}
			}
		}
		return result.toString();
	}

	
	/**
	 * Answers class loader composed of node's plugin class loader and current runtime context class loader.
	 * 
	 * @param node
	 * @return
	 */
	public static ClassLoader createNodeClassLoader(Node node) {
		
		ArrayList<ClassLoader> parentClassLoaders = new ArrayList<ClassLoader>();
		parentClassLoaders.add(node.getClass().getClassLoader());
		parentClassLoaders.addAll(DynamicCompiler.getCTLLibsClassLoaders());
		
		IAuthorityProxy authorityProxy = node.getAuthorityProxy();

		ClassLoader parentClassLoader = authorityProxy.createMultiParentClassLoader(parentClassLoaders.toArray(new ClassLoader[parentClassLoaders.size()]));
		URL[] runtimeClasspath = node.getGraph().getRuntimeContext().getRuntimeClassPath();
		return authorityProxy.createClassLoader(runtimeClasspath, parentClassLoader, true);
	}

	/**
	 * Creates a URL class loader for provided class path (relative to the provided context URL).
	 * Thread context class loader is used as the parent of the result.
	 * If the class path is empty, thread context class loader is returned.
	 * 
	 * @param contextUrl
	 * @param classpath
	 * @return
	 */
	public static ClassLoader createURLClassLoader(URL contextUrl, String classpath) {
		ClassLoader classLoader;
		if (UnicodeBlanks.isBlank(classpath)) {
			classLoader = Thread.currentThread().getContextClassLoader();
		} else {
			try {
				final URL urls[] = getClassLoaderUrls(contextUrl, classpath);
				classLoader = AccessController.doPrivileged(new PrivilegedExceptionAction<ClassLoader>() {

					@Override
					public ClassLoader run() {
						return ContextProvider.getAuthorityProxy().createClassLoader(urls, Thread.currentThread().getContextClassLoader(), false);
					}
				});
			} catch (MalformedURLException e) {
				throw new JetelRuntimeException(e);
			} catch (PrivilegedActionException e) {
				throw new JetelRuntimeException(e);
			}
		}

		return classLoader;
	}

	public static URL[] getClassLoaderUrls(URL contetxtUrl, String classpath) throws MalformedURLException {
		return getClassLoaderUrls(contetxtUrl, classpath, Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
	}
	
	/**
	 * Converts given class path in string form to array of URLs that
	 * are suitable for class loader initialization.
	 * The method ensures that directory paths contain trailing slash
	 * so that URLClassLoader will treat is as directory.
	 * Sandbox URLs are converted into basic file protocol if possible.
	 * 
	 * @param contextUrl - URL to which relative class path items are resolved
	 * @param classpath - string containing list of resources providing JAR files/class folders
	 * @param separator - character separating items in class path
	 * 
	 */
	public static URL[] getClassLoaderUrls(URL contextUrl, String classpath, String separator) throws MalformedURLException {
		
		String paths[] = classpath.split(separator);
		URL urls[] = FileUtils.getFileUrls(contextUrl, paths);
		/*
		 * resolve sandbox URLs
		 */
		for (int i = 0; i < urls.length; ++i) {
			if (SandboxUrlUtils.isSandboxUrl(urls[i])) {
				URL localUrl = SandboxUrlUtils.toLocalFileUrl(urls[i]);
				if (localUrl != null) {
					// sandbox URL can be replaced by URL of a local file
					urls[i] = localUrl;
				} else {
					// sandbox URLs to remote sandboxes will be processed on server-side of AuthorityProxy
				}
			}
		}
		for (int i = 0; i < urls.length; ++i) {
			urls[i] = cleanURL(urls[i]);
		}// for
		return urls;
	}
	
	/**
	 * Utility to transform local file URL pointing to a directory to the form
	 * with trailing slash, so that it correctly recognized as a class files folder
	 * by {@link URLClassLoader}.
	 * 
	 * @param localUrl
	 * @return
	 * @throws MalformedURLException 
	 */
	public static URL cleanURL(URL url) throws MalformedURLException {
		if ("file".equals(url.getProtocol())) {
			File file = FileUtils.convertUrlToFile(url);
			if (file.isDirectory() && !url.toString().endsWith("/")) {
				url = new URL(url.toString() + "/");
			}
		}
		return url;
	}
	
    /**
     * Instantiates class from the given className.
     * 
     * @throws LoadClassException
     */
    public static <T> T loadClassInstance(Class<T> expectedType, String className, Node node) {
    	Object instance = loadClassInstance(className, node);
    	try {
    		return expectedType.cast(instance);
    	} catch (ClassCastException e) {
    		throw new LoadClassException("Provided class '" + className + "' does not extend/implement " + expectedType.getName(), e);
    	}
    }
    
    /**
     * Instantiates class from the given className.
     * @throws LoadClassException
     */
    public static <T> T loadClassInstance(Class<T> expectedType, String className, ClassLoader cl) {
    	Object instance = loadClassInstance(className, cl);
    	try {
    		return expectedType.cast(instance);
    	} catch (ClassCastException e) {
    		throw new LoadClassException("Provided class '" + className + "' does not extend/implement " + expectedType.getName(), e);
    	}
    }

    /**
     * Instantiates class from the given className.
     * @throws LoadClassException
     */
    public static Object loadClassInstance(String className, Node node) {
    	return loadClassInstance(className, ClassLoaderUtils.createNodeClassLoader(node));
    }
    
    /**
     * Instantiates class of the given className using provided class loader.
     * @throws LoadClassException
     */
    public static Object loadClassInstance(String className, ClassLoader loader) {
    	try {
    		Class<?> klass = Class.forName(className, true, loader);
    		return klass.newInstance();
    	} catch (ClassNotFoundException e) {
    		throw new LoadClassException("Cannot find class: " + className, e);
    	} catch (IllegalAccessException e) {
    		throw new LoadClassException("Cannot instantiate class: " + className, e);
    	} catch (InstantiationException e) {
    		throw new LoadClassException("Cannot instantiate class: " + className, e);
    	} catch (ExceptionInInitializerError e) {
    		throw new LoadClassException("Cannot initialize class: " + className, e);
    	} catch (LinkageError e) {
    		throw new LoadClassException("Cannot link class: " + className, e);
    	} catch (SecurityException e) {
    		throw new LoadClassException("Cannot instantiate class: " + className, e);
    	}
    }
    
	private static URL[] getClasspathUrls(ClassLoader loader) {
		
		if (loader instanceof URLBasedClassLoader) {
			URLBasedClassLoader ucl = (URLBasedClassLoader)loader;
			return ucl.getURLs();
			
		} else if (loader instanceof URLClassLoader) {
			URLClassLoader ucl = (URLClassLoader)loader;
			return ucl.getURLs();
			
		} else if (loader == ClassLoader.getSystemClassLoader()) {
			return getApplicationClasspathUrls();
		}
		return new URL[0];
	}
	
    /**
     * Answers normalized path to local file/directory represented by given URL.
     * @param url
     * @return file path or empty string if the URL does represent JAR file or a directory
     */
	private static String getClasspathFilePath(URL url) {
		
		String filePath;
		try {
			filePath = URLDecoder.decode(url.getPath(), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new JetelRuntimeException(e);
		}

		if (isSuitableClasspathFile(filePath)) {
			/*
			 *  JDK 9 does not recognize a leading slash in a file URI as a valid class path in Windows.
			 *  https://bugs.java.com/view_bug.do?bug_id=JDK-8185596
			 */
			return filePath.replaceFirst("^/(.:/)", "$1");
		}

		filePath = filePath.substring(1);
		if (isSuitableClasspathFile(filePath)) {
			return filePath;
		}

		filePath = url.getFile();
		if (isSuitableClasspathFile(filePath)) {
			return filePath;
		}

		return "";
	}

	private static boolean isSuitableClasspathFile(String filePath) {
		
		File file = new File(filePath);
		try {
			if (file.exists() && (filePath.endsWith(".jar") || filePath.endsWith(".zip") || file.isDirectory())) {
				return true;
			}
		} catch (AccessControlException e) {
			if (logger.isDebugEnabled()) {
				logger.debug("Cannot access file " + file, e);
			}
		}
		return false;
	}

	/**
	 * Answers URLs forming class path of the running JVM.
	 * @return
	 */
	private static URL[] getApplicationClasspathUrls() {
		URL[] urls = new URL[0];
		try {
			urls = getClassLoaderUrls(null, System.getProperty("java.class.path"), System.getProperty("path.separator"));
		} catch (MalformedURLException e) {
			if (logger.isDebugEnabled()) {
				logger.debug("Cannot parse java.class.path: " + e.getMessage(), e);
			}
		}
		return urls;
	}
}
