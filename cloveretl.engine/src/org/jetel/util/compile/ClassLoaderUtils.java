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
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.LoadClassException;
import org.jetel.graph.Node;
import org.jetel.util.classloader.MultiParentClassLoader;
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.SandboxUrlUtils;
import org.jetel.util.string.StringUtils;

/**
 * @author David Pavlis, JavlinConsulting, s.r.o.
 * @since 27.3.2006
 * 
 * 
 */

public class ClassLoaderUtils {

	static Log logger = LogFactory.getLog(ClassLoaderUtils.class);

	public static String getClasspath(ClassLoader loader, URL... classPathUrls) {
		URL[] urls = null;

		try {
			// this method is used by org.jboss.mx.loading.RepositoryClassLoader
			Method getAllURLs = loader.getClass().getMethod("getAllURLs");
			urls = (URL[]) getAllURLs.invoke(loader);
		} catch (Throwable ex) {
			// ignore
		}

		if (urls == null || urls.length == 0) {
			try {
				// this way non-URLClassLoader or extended ClassLoaders can also return urls
				Method getAllURLs = loader.getClass().getMethod("getURLs");
				urls = (URL[]) getAllURLs.invoke(loader);
			} catch (Throwable ex) {
				// ignore
			}
		}

		if (urls == null || urls.length == 0) {
			urls = classPathUrls;
		} else if (classPathUrls != null && classPathUrls.length != 0) {
			URL[] cpUrls = new URL[urls.length + classPathUrls.length];
			System.arraycopy(urls, 0, cpUrls, 0, urls.length);
			System.arraycopy(classPathUrls, 0, cpUrls, urls.length, classPathUrls.length);

			urls = cpUrls;
		}

		String classpath = "";

		if (urls != null) {
			StringBuilder classPathBuilder = new StringBuilder();

			for (int i = 0; i < urls.length; i++) {
				String fileName = getCheckedFileName(urls[i]);

				if (fileName.length() > 0) {
					classPathBuilder.append(File.pathSeparator);
					classPathBuilder.append(fileName);
				}
			}

			if (classPathBuilder.length() > 0) {
				classpath = classPathBuilder.substring(File.pathSeparator.length());
			}
		}

		return classpath;
	}

	private static String getCheckedFileName(URL url) {
		String fileName;

		try {
			fileName = URLDecoder.decode(url.getPath(), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			logger.error("Unsupported encoding UTF-8: " + e.toString());
			return "";
		}

		if (isFileOk(fileName)) {
			return fileName;
		}

		fileName = fileName.substring(1);
		if (isFileOk(fileName)) {
			return fileName;
		}

		fileName = url.getFile();
		if (isFileOk(fileName)) {
			return fileName;
		}

		return "";
	}

	private static boolean isFileOk(String fileName) {
		File file = new File(fileName);
		if (file.exists() && (fileName.endsWith(".jar") || fileName.endsWith(".zip") || file.isDirectory())) {
			return true;
		}
		return false;
	}

	/**
	 * Returns instance of GreedyURLClassLoader able to load classes from specified locations.
	 * 
	 * @see GreedyURLClassLoader
	 * @param contextURL
	 * @param libraryPaths
	 * @return
	 */
	/*
	public static GreedyURLClassLoader createClassLoader(ClassLoader parentCl, URL contextURL, URL[] libraryPaths) {
		return new GreedyURLClassLoader(libraryPaths, parentCl);
	}*/
	
	/**
	 * Answers class loader composed of the node's plugin classloader and current runtime context
	 * class loader.
	 * @param node
	 * @return
	 */
	public static ClassLoader createNodeClassLoader(Node node) {
		ArrayList<ClassLoader> parentClassLoaders = new ArrayList<ClassLoader>();
		parentClassLoaders.add(node.getClass().getClassLoader());
		parentClassLoaders.addAll(DynamicCompiler.getCTLLibsClassLoaders());
		
		ClassLoader parentClassLoader = new MultiParentClassLoader(parentClassLoaders.toArray(new ClassLoader[0]));
		URL[] runtimeClasspath = node.getGraph().getRuntimeContext().getRuntimeClassPath();
		return node.getGraph().getAuthorityProxy().createClassLoader(runtimeClasspath, parentClassLoader, true);
	}

	public static ClassLoader createURLClassLoader(URL contextUrl, String classpath) {
		ClassLoader classLoader;
		if (StringUtils.isEmpty(classpath)) {
			classLoader = Thread.currentThread().getContextClassLoader();
		} else {
			try {
				final URL urls[] = getClassloaderUrls(contextUrl, classpath);
				classLoader = AccessController.doPrivileged(new PrivilegedExceptionAction<ClassLoader>() {

					@Override
					public ClassLoader run() {
						return new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
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

	/*
	public static ClassLoader createClassLoader(List<URL> urls, ClassLoader parent, boolean greedy) {
		URL[] urlsArray = null;
		if (urls != null)
			urlsArray = new URL[urls.size()];
		return createClassLoader(urls == null ? null : urls.toArray(urlsArray), parent, greedy);
	}*/
	
	/*
	public static ClassLoader createClassLoader(URL[] urls, ClassLoader parent, boolean greedy) {
		if (parent == null) {
			parent = PrimitiveAuthorityProxy.class.getClassLoader();
		}
        if (urls == null || urls.length == 0) {
        	return parent;
        } else {
        	if (greedy) {
        		return new GreedyURLClassLoader(urls, parent);
        	} else {
        		return new URLClassLoader(urls, parent);
        	}
        }
	}*/

	/**
	 * Convert the given classpath in string form to array of URLs, which
	 * are suitable for classloader initialization.
	 * The method ensures that directory paths contains '/' at the end
	 * so that URLClassLoader will treat is as directory.
	 * Sandbox urls are converted into basic file protocol, if possible.
	 */
	public static URL[] getClassloaderUrls(URL contextUrl, String classpath) throws MalformedURLException {
		String paths[] = classpath.split(Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
		URL urls[] = FileUtils.getFileUrls(contextUrl, paths);
		/*
		 * resolve sandbox URL's
		 */
		for (int i = 0; i < urls.length; ++i) {
			if (SandboxUrlUtils.isSandboxUrl(urls[i])) {
				URL localUrl = SandboxUrlUtils.toLocalFileUrl(urls[i]);
				if (localUrl != null) {
					// sandbox URL can be replaced by URL of local file
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
    public static Object loadClassInstance(String className, Node node) {
    	return loadClassInstance(className, ClassLoaderUtils.createNodeClassLoader(node));
    }
    
    /**
     * Instantiates class from the given className.
     * @throws LoadClassException
     */
    public static Object loadClassInstance(String className, ClassLoader loader) {
    	try {
    		Class<?> klass = Class.forName(className, true, loader);

    		//temporary logging, which should be removed after fixing failing test 'RunGraph_CLO-2734' 
    		logger.debug("Class found: " + klass.getName());
    		logger.debug("ClassLoader name: " + klass.getClassLoader().getClass().getName());
    		logger.debug("ClassLoader urls: " + getClasspath(klass.getClassLoader(), (URL[]) null));
    		logger.debug("ClassLoader.parent name: " + klass.getClassLoader().getParent().getClass().getName());
    		logger.debug("ClassLoader.parent urls: " + getClasspath(klass.getClassLoader().getParent(), (URL[]) null));
    		
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

	public static class SandboxURLException extends RuntimeException {
		
		private static final long serialVersionUID = 1L;
		
		public SandboxURLException(String message, Throwable cause) {
			super(message, cause);
		}
	}

}
