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
import com.sun.tools.javac.Main;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.database.SQLUtil;
import org.jetel.exception.*;

/**
 * @author      Wes Maciorowski, David Pavlis
 * @since
 * @revision    $Revision$
 */
public class Compile {

	private final static String compilerClassname = "sun.tools.javac.Main";
	private final static String compilerExecutable = "javac";

	private String srcFile;
	private String destDir;
	private String fileSeparator;
	private String errFilename;
	private boolean forceCompile;
	private boolean compiled;
	private boolean useExecutable = false;

	static Log logger = LogFactory.getLog(Compile.class);

	/**
	 *Constructor for the JavaCompiler object
	 *
	 * @param  srcFile  Description of the Parameter
	 */
	public Compile(String srcFile) {
		this.srcFile = srcFile;
		destDir = System.getProperty("java.io.tmpdir", ".");
		forceCompile = false;
		compiled = false;
		fileSeparator = System.getProperty("file.separator", "/");
	}


	/**
	 *Constructor for the JavaCompiler object
	 *
	 * @param  srcFile  Description of the Parameter
	 * @param  destDir  Description of the Parameter
	 */
	public Compile(String srcFile, String destDir) {
		this.srcFile = srcFile;
		this.destDir = destDir;
		forceCompile = false;
		compiled = false;
		fileSeparator = System.getProperty("file.separator", "/");
	}


	/**
	 *  Sets the forceCompile attribute of the JavaCompiler object
	 *
	 * @param  forceCompile  The new forceCompile value
	 */
	public void setForceCompile(boolean forceCompile) {
		this.forceCompile = forceCompile;
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
	public int compile() {
		int status = 0;
		File source;
		try{
			source = new File(srcFile);
		}catch(Exception ex){
			throw new RuntimeException(ex.getMessage());
		}
		
		// just try that we can reach compiler
		try{
			Class.forName(compilerClassname);
		}catch(ClassNotFoundException ex){
			useExecutable=true;
			logger.warn("..can't locate class "+compilerClassname+" - will use external javac");
		}
			
			
		if (forceCompile || needsRecompile()) {
			errFilename=destDir
				+ (destDir.endsWith(fileSeparator)
				  ? ""
				  : fileSeparator)
				+ source.getName()
				+ ".err";
			if (useExecutable) {
				String[] args = new String[]{compilerExecutable, "-d", destDir, srcFile,
						"-Xstdout", errFilename };
				Runtime runtime = Runtime.getRuntime();
				try{
					status = runtime.exec(args).waitFor();
				}catch(Exception ex){
					status=-1;
				}

			} else {
				String[] args = new String[]{"-d", destDir, srcFile,
						"-Xstdout", errFilename};

				status = com.sun.tools.javac.Main.compile(args);

			}
			if (status==0) { 
			compiled = true;
			}
		}
		
		return status;
	}


	/**
	 *  Gets the compiledClassPath attribute of the JavaCompiler object
	 *
	 * @return    The compiledClassPath value
	 */
	public String getCompiledClassPath() {
		return destDir;
	}

	/**
	 * Method which returns complete path to
	 * file containing error output of Javac
	 * 
	 * @return path to error output file
	 */
	public String getErrFilename(){
		return errFilename;
	}
	
	
	/**
	 *  Description of the Method
	 *
	 * @return    true if souce file needs recompiling(was modified after compilation), otherwise false
	 */
	private boolean needsRecompile() {
		try {
			File source = new File(srcFile);
			int index=source.getName().lastIndexOf('.');
			String className=source.getName().substring(0,index);
			// we need to conver blblabl.java to blablab.class to compare files
			File dest = new File(destDir
				+ (destDir.endsWith(fileSeparator)
				  ? ""
				  : fileSeparator)
				+ className
				+ ".class");
			if (dest.exists() && (dest.lastModified() >= source.lastModified())) {
				return false;// is already compiled !!!!
			} else {
				return true;
			}

		} catch (Exception ex) {
			return true;// needs recompile
		}
	}


	
}

