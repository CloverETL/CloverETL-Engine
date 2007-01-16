/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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
*/
package org.jetel.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Helper class for dynamic compiling of Java source code. Offers instantiating of compiled code.
 *
 *
 * @author      David Pavlis
 * @since       21. January 2004
 * @revision    $Revision$
 */
public class DynamicJavaCode {
    private final static String SRC_PATH = System.getProperty("java.io.tmpdir", ".");
    private final static String FILE_SEPARATOR = System.getProperty("file.separator", "/");
    private final static Pattern PATTERN = Pattern.compile("class\\s+(\\w+)"); 

    private static Log logger = LogFactory.getLog(DynamicJavaCode.class);

    private String srcPath;
	private String srcCode;
	private String className;
	private String fileName;
	private String compilerOutput;
	private ClassLoader classLoader;
    
	private boolean captureCompilerOutput = true; 
	
	public DynamicJavaCode(String srcCode, ClassLoader classLoader) {
        this.classLoader = classLoader;
		this.srcCode = srcCode;
        this.srcPath = SRC_PATH + (SRC_PATH.endsWith(FILE_SEPARATOR) ? "" : FILE_SEPARATOR);
		Matcher matcher = PATTERN.matcher(srcCode);
		if (!matcher.find()) {
			throw new RuntimeException("Can't find class name within source code !");
		}
		className = matcher.group(1);
		if (className.length() == 0) {
			throw new RuntimeException("Can't extract class name from source code !");
		}
	}

    public DynamicJavaCode(String srcCode) {
        this(srcCode, null);
    }

	private void saveSrc() {
		long checkSumFile;
		fileName = srcPath + className + ".java";
		Checksum checkSumSrc=new Adler32();
		byte[] stringBytes=srcCode.getBytes();
		checkSumSrc.update(stringBytes,0,stringBytes.length);
		// try to get checksum of already (may be) saved src)
		checkSumFile=FileUtils.calculateFileCheckSum(fileName);
		
		// do we need to save src ? 
		if (checkSumFile!=checkSumSrc.getValue()){
			try{
				FileWriter writer=new FileWriter(fileName,false);
				writer.write(srcCode);
				writer.close();
			}catch(IOException ex){
			    logger.error("Error when trying to save source code: "+ex.getMessage());
				throw new RuntimeException("Error when trying to save source code: "+ex.getMessage());
			}
		}
	}
	
	private void compile() {
		Compiler compiler = new Compiler(fileName, captureCompilerOutput);
        compiler.setClassLoader(classLoader);
		int result = compiler.compile();
		if (result != 0) {
		    compilerOutput = compiler.getCapturedOutput();
			StringBuffer errMessage = new StringBuffer("Error(s) when compiling: ");
			errMessage.append(fileName).append("\n");
			if (!captureCompilerOutput){
			    errMessage.append(" - compiler output can be found in: ").append(compiler.getErrFileName());
			}else{
			    errMessage.append(compilerOutput);
			}
			throw new RuntimeException(errMessage.toString()); 
		}
	}
	
	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
	public Object instantiate() {
		Class tClass;
		String urlString = "file:" + srcPath;
		URL[] myURLs;
		
		// firstly, save source
		saveSrc();
        
		// secondly, try to compile it
		compile();
		
		// now, load in class file 
		try {
			myURLs = new URL[]{new URL(urlString)};
		} catch (MalformedURLException ex) {
			throw new RuntimeException(ex);
		}

        URLClassLoader classLoader = new URLClassLoader(myURLs, getClassLoader());
		try {
		    logger.debug("Loading Class: " + className + "...");
			tClass = Class.forName(className, true, classLoader);
			logger.debug("Class: " + className + " Loaded");
		} catch (ClassNotFoundException ex) {
		    logger.error("Can not find class: " + ex);
			throw new RuntimeException("Can not find class: " + className + " : " + ex);
		} catch (Exception ex) {
		    logger.error(ex);
			throw new RuntimeException(ex);
		}
		Object myObject;
		try {
			myObject = tClass.newInstance();
		} catch (Exception ex) {
			logger.error("Error when creating object of class " + className + " : " + ex.getMessage());
            throw new RuntimeException(ex);
		}

		return myObject;
	}


	/**
	 *  Gets the className attribute of the DynamicJavaCode object
	 *
	 * @return    The className value
	 */
	public String getClassName() {
		return className;
	}


	/**  Deletes dynamicaly created file. */
	public void clean() {
		try {
            if(fileName != null) {
                new File(fileName).delete();
            }
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	/**
	 * Returns Java source code from which this object has been compiled
	 * @return String with source code
	 */
	public String getSourceCode() {
		return(this.srcCode);
	}
	
    /**
     * Returns output of compiler for the last compilation
     * of Java source code.<br>
     * Only when captureCompilerOutput is set to true!
     * 
     * @return compiler's output (both STDOUT & STDERR)
     */
    public String getCompilerOutput() {
        return compilerOutput;
    }
    
    /**
     * @return status of capturing compiler's output
     */
    public boolean isCaptureCompilerOutput() {
        return captureCompilerOutput;
    }
    /**
     * Sets on/off capturing compiler's output
     * 
     * @param captureCompilerOutput
     */
    public void setCaptureCompilerOutput(boolean captureCompilerOutput) {
        this.captureCompilerOutput = captureCompilerOutput;
    }

    public void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    public ClassLoader getClassLoader() {
        if(classLoader == null) {
            //return Thread.currentThread().getContextClassLoader();
            return getClass().getClassLoader();
        }
        return classLoader;
    }
}

