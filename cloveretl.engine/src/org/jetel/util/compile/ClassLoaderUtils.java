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
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.classloader.GreedyURLClassLoader;
import org.jetel.util.file.FileUtils;
import org.jetel.util.string.StringUtils;


/**
 * @author David Pavlis, JavlinConsulting, s.r.o.
 * @since  27.3.2006
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
        if (file.exists()
                && (fileName.endsWith(".jar")
                        || fileName.endsWith(".zip")
                        || file.isDirectory())) {
            return true;
        }
        return false;
    }
    
    /**
     * Returns instance of GreedyURLClassLoader able to load classes from specified locations.
     * @see GreedyURLClassLoader
     * @param contextURL
     * @param libraryPaths
     * @return
     * @throws ComponentNotReadyException
     */
	public static GreedyURLClassLoader createClassLoader(ClassLoader parentCl, URL contextURL, URL[] libraryPaths)	throws ComponentNotReadyException {
		return new GreedyURLClassLoader(libraryPaths, parentCl);
	}
	
	public static ClassLoader createURLClassLoader(URL contextUrl, String classpath) throws ComponentNotReadyException {
		ClassLoader classLoader;
		if (StringUtils.isEmpty(classpath)) {
			classLoader = Thread.currentThread().getContextClassLoader();
		} else{
			try {
				final URL urls[] = getClassloaderUrls(contextUrl, classpath);
				classLoader = AccessController.doPrivileged(new PrivilegedExceptionAction<ClassLoader>() {

					@Override
					public ClassLoader run() {
						return new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
					}
				});
			} catch (MalformedURLException e) {
				throw new ComponentNotReadyException(e);
			} catch (URISyntaxException e) {
				throw new ComponentNotReadyException(e);
			} catch (PrivilegedActionException e) {
				throw new ComponentNotReadyException(e);
			}
		}
		
		return classLoader;
	}
	
	/**
	 * Ensures that directory paths contains '/' at the end so that URLClassLoader will treat is as directory.
	 */
	private static URL[] getClassloaderUrls(URL contextUrl, String classpath) throws MalformedURLException, URISyntaxException {
		String paths[] = classpath.split(Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
		URL urls[] = FileUtils.getFileUrls(contextUrl, paths);
		for (int i = 0; i < urls.length; ++i) {
			if (new File(urls[i].toURI()).isDirectory() && !urls[i].toString().endsWith("/")) {
				urls[i] = new URL(urls[i].toString() + "/");
			}
		}
		return urls;
	}
}
