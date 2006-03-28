/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-05  David Pavlis <david_pavlis@hotmail.com> and others.
 *    
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *    
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
 *    Lesser General Public License for more details.
 *    
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Created on 27.3.2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */

package org.jetel.util;

import java.net.URLClassLoader;
import java.net.URL;
import java.io.File;
import java.lang.reflect.Method;


/**
 * @author not attributable
 * @since  27.3.2006
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */

public class ClassLoaderUtils {
    public static String getClasspath(ClassLoader loader) {
        String classpath = "";
        URL[] urls = null;
        
        try {
            // this method is used by org.jboss.mx.loading.RepositoryClassLoader
            Class[] sig = {};
            Method getAllURLs = loader.getClass().getMethod("getAllURLs", sig);
            Object[] args = {};
            urls = (URL[]) getAllURLs.invoke(loader, args);
        } catch (Exception ex) {
            // ignore
        }
        try{
            if (urls == null || urls.length == 0) {
                // this way non-URLClassLoader or extended ClassLoaders can also return urls
                Class[] sig = {};
                Object[] args = {};
                Method getAllURLs = loader.getClass().getMethod("getURLs", sig);
                urls = (URL[]) getAllURLs.invoke(loader, args);
            }
        }
        catch(Exception ex){
            // ignore
        }
        
        if (urls != null) {
            StringBuffer classPathBuff = new StringBuffer();
            for (int i = 0; i < urls.length; i++) {
                String fileName = getCheckedFileName(urls[i]);
                if (fileName.length() > 0) {
                    if (classPathBuff.length() > 0) {
                        classPathBuff.append(";");
                    }
                    classPathBuff.append(fileName);
                }
            }
            classpath = classPathBuff.toString();
        }
        
        return classpath;
    }
    
    private static String getCheckedFileName(URL url) {
        String fileName;
        
        fileName = url.getPath();
        
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
    
}
