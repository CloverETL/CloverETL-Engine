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
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.net.URLClassLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author      Wes Maciorowski, David Pavlis
 * @since
 * @revision    $Revision$
 */
public class Compile {

	private final static String COMPILER_CLASSNAME = "com.sun.tools.javac.Main";
	private final static String COMPILER_EXECUTABLE = "javac";
	private final static int DEFAULT_BYTE_ARRAY_SIZE=512;

	private String srcFile;
	private String destDir;
	private String fileSeparator;
	private String errFilename;
	private String capturedOutput;
	private boolean forceCompile;
	private boolean compiled;
	private boolean useExecutable = false;
	private boolean captureSTDOUT = false;

	static Log logger = LogFactory.getLog(Compile.class);

	/**
	 *Constructor for the JavaCompiler object
	 *
	 * @param  srcFile  path to source code file which has to be compiled
	 */
	public Compile(String srcFile) {
		this.srcFile = srcFile;
		destDir = System.getProperty("java.io.tmpdir", ".");
		forceCompile = false;
		compiled = false;
		fileSeparator = System.getProperty("file.separator", "/");
	}

	/**
	 * 
	 * @param srcFile Java source file to compile
	 * @param captureOutput should compiler's output (to STDOUT & STDERR) be captured ?
	 */
	public Compile(String srcFile,boolean captureOutput){
	    this(srcFile);
	    this.captureSTDOUT=captureOutput;
	}
	
	
	/**
	 *Constructor for the JavaCompiler object
	 *
	 * @param  srcFile  path to source code file which has to be compiled
	 * @param  destDir  destination directory where output (compiled) class should be stored
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
		PrintStream savedOut=null;
		PrintStream savedErr=null;
		String[] args;
		ByteArrayOutputStream outputStream=null;
		try{
			source = new File(srcFile);
		}catch(Exception ex){
			throw new RuntimeException(ex.getMessage());
		}
		
		// just try that we can reach compiler
		Class cl = null;
		Class cl1 = null;
		URLClassLoader classLoader = null;
		try{
			Class.forName(COMPILER_CLASSNAME);
		}catch(ClassNotFoundException ex){
			useExecutable=true;
			logger.warn("..can't locate class "+COMPILER_CLASSNAME+" - will use external javac");
		} catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
			
			
		if (forceCompile || needsRecompile()) {
			errFilename=destDir
				+ (destDir.endsWith(fileSeparator)
				  ? ""
				  : fileSeparator)
				+ source.getName()
				+ ".err";
			if (useExecutable) {
			    Runtime runtime = Runtime.getRuntime();  
			    if (captureSTDOUT){
			        args = new String[]{COMPILER_EXECUTABLE, "-d", destDir, srcFile};
			        savedOut=System.out;
			        savedErr=System.err;
			        outputStream=new ByteArrayOutputStream(DEFAULT_BYTE_ARRAY_SIZE);
			        PrintStream out=new PrintStream(new BufferedOutputStream(outputStream));
			        System.setErr(out);
			        System.setOut(out);
			        
			    }else{
			        args = new String[]{COMPILER_EXECUTABLE, "-d", destDir, srcFile,
						"-Xstdout", errFilename };
			    }
				
				try{
					status = runtime.exec(args).waitFor();
				}catch(Exception ex){
					status=-1;
				}
				
				if (captureSTDOUT){
                    System.setErr(savedErr);
                    System.setOut(savedOut);
                }

			} else {
			    if (captureSTDOUT){
			        args = new String[]{"-d", destDir, srcFile};
			        savedOut=System.out;
			        savedErr=System.err;
			        outputStream=new ByteArrayOutputStream(DEFAULT_BYTE_ARRAY_SIZE);
			        PrintStream out=new PrintStream(new BufferedOutputStream(outputStream));
			        System.setErr(out);
			        System.setOut(out);
			        
			    }else{
			        args = new String[]{"-d", destDir, srcFile,
						"-Xstdout", errFilename};
			    }

                        status = com.sun.tools.javac.Main.compile(args);
                        //status = ((Integer) cl.getMethod("compile", new Class[] {String[].class}).invoke(null, new Object[] {args})).intValue();
                 if (captureSTDOUT){
                     System.setErr(savedErr);
                     System.setOut(savedOut);
                 }
                       
			}
			if (status==0) { 
			compiled = true;
			}
		}
		if (captureSTDOUT){
		    this.capturedOutput=outputStream.toString();
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


	
    public boolean isCaptureSTDOUT() {
        return captureSTDOUT;
    }
    public void setCaptureSTDOUT(boolean captureSTDOUT) {
        this.captureSTDOUT = captureSTDOUT;
    }
    public String getCapturedOutput() {
        return capturedOutput;
    }
}

