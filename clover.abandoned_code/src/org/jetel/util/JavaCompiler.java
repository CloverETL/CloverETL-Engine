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
import org.jetel.exception.*;

/**
 * @author      Wes Maciorowski, David Pavlis
 * @since
 * @revision    $Revision$
 */
public class JavaCompiler {

	private final static String compilerClassname = "sun.tools.javac.Main";
	private final static String compilerExecutable = "javac";

	private String srcFile;
	private String errFile;
	private String destDir;
	private String fileSeparator;
	private String className;
	private boolean forceCompile;
	private boolean compiled;
	private boolean useJavac = false;
	
	/**
	 *Constructor for the JavaCompiler object
	 *
	 * @param  srcFile  Description of the Parameter
	 */
	public JavaCompiler(String srcFile) {
		this.srcFile = srcFile;
		destDir = System.getProperty("java.io.tmpdir", ".");
		forceCompile = false;
		compiled = false;
		fileSeparator = System.getProperty("file.separator", "/");
		className = new File(srcFile).getName().replaceAll("(\\.java)$", "");
	}


	/**
	 *Constructor for the JavaCompiler object
	 *
	 * @param  srcFile  Description of the Parameter
	 * @param  destDir  Description of the Parameter
	 */
	public JavaCompiler(String srcFile, String destDir) {
		this.srcFile = srcFile;
		this.destDir = destDir;
		forceCompile = false;
		compiled = false;
		fileSeparator = System.getProperty("file.separator", "/");
		className = new File(srcFile).getName().replaceAll("(\\.java)$", "");
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
		if (needsRecompile() || forceCompile) {
			errFile=destDir+fileSeparator+ className + ".err";
			if (useJavac) {
				String[] args = new String[]{compilerExecutable, "-deprecation","-d", destDir, srcFile,
						"-Xstdout", errFile};
				Runtime runtime = Runtime.getRuntime();
				try {
					status = runtime.exec(args).waitFor();
				} catch (IOException ex) {
					status = -1;
				} catch (InterruptedException ex) {
					status = -1;
				}

			} else {
				String[] args = new String[]{"-deprecation","-d", destDir, srcFile,
						"-Xstdout", errFile};
				status = com.sun.tools.javac.Main.compile(args);

			}
		}
		compiled = true;
		return status;
	}


	/**
	 *  Gets the compiledClassPath attribute of the JavaCompiler object
	 *
	 * @return    The path where compiled class resides
	 */
	public String getCompiledClassPath() {
		return destDir;
	}


	/**
	 *  Gets the compiledClassName attribute of the JavaCompiler object
	 *
	 * @return    compiled class name
	 */
	public String getCompiledClassName() {
		return className;
	}

	public void cleanUp(){
		try {
			new File(destDir+fileSeparator+className+".class").delete();
			new File(destDir+fileSeparator+className+".err").delete();
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	public String getErrFile(){
		return errFile;
	}

	/**
	 *  Description of the Method
	 *
	 * @return    true if souce file needs recompiling(was modified after compilation), otherwise false
	 */
	private boolean needsRecompile() {
		File source = new File(srcFile);
		File dest = new File(destDir + fileSeparator + className + ".class");
		try {
			if (dest.exists() && (dest.lastModified() >= source.lastModified())) {
				return false;// is already compiled
			} else {
				return true;
			}

		} catch (Exception ex) {
			return true;// needs recompile
		}
	}


	/* FOLLOWING CODE IS NOT NECESSARY
	 *  static{
	 *  / Find compiler class:
	 *  try
	 *  {
	 *  compilerClass = Class.forName(compilerClassname);
	 *  compilerConstructor = compilerClass.getConstructor(constructorSignature);
	 *  / Get the method "compile(String[] arguments)".
	 *  / The method has the same signature on the classic and modern
	 *  / compiler, but they have different return types (boolean/int).
	 *  / Since the return type is ignored here, it doesn't matter.
	 *  Class[] methodSignature = { String[].class };
	 *  compilerMethod = compilerClass.getMethod("compile", methodSignature);
	 *  }
	 *  catch (ClassNotFoundException cnf)
	 *  {
	 *  }
	 *  catch (Exception e)
	 *  {
	 *  }
	 *  }
	 */
	 
	 /* for testing only !!
	 public static void main(String[] args){
		 
		 System.out.println("Output: "+new JavaCompiler(args[0],args[1]).compile());
	 }
	 */

}

