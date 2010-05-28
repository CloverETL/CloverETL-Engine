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

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.util.string.StringUtils;

/**
 * @author Martin Zatopek, JavlinConsulting,s.r.o.
 * @since  18.8.2006
 *
 * Class allowing to compile Java source code into bytecode representation (.class file)
 */
public class Compiler {
    private final static String COMPILER_CLASSNAME = "com.sun.tools.javac.Main";
    private final static int DEFAULT_BYTE_ARRAY_BUFFER_SIZE = 512;
    
    private static Log logger = LogFactory.getLog(Compiler.class);

    private String srcFileName;
    private String destFileName;
    private String destDirName;
    private String errFileName;
    private File srcFile;
    private File destFile;
    private boolean captureOutput;
    private boolean forceRecompile;
    private String classPath;
    private ByteArrayOutputStream compileOutputStream;
    private ClassLoader classLoader;
    
    public Compiler(String srcFileName, boolean captureOutput, String destDirName) {
        this.srcFileName = srcFileName;
        this.destDirName = destDirName;
        this.captureOutput = captureOutput;
        
        this.srcFile = new File(srcFileName);
        
        //create destFileName from srcFileName - "/src_dir/trans.java" --> "/dest_dir/trans.class"
        int index = srcFile.getName().lastIndexOf('.');
        String className = srcFile.getName().substring(0, index);
        this.destFileName = className + ".class";
        this.destFile = new File(destDirName, destFileName);
        
        //create errFileName
        errFileName = destDirName + className + ".err";
    }
    
    public Compiler(String srcFile, boolean captureOutput) {
        this(srcFile, captureOutput, System.getProperty("java.io.tmpdir", "."));
    }

    public Compiler(String srcFile) {
        this(srcFile, false);
    }
    
    public int compile() {
        if(needRecompile()) {
            try {
                Class.forName(COMPILER_CLASSNAME);
            } catch (ClassNotFoundException ex) {
                logger.warn("Can't locate class " + COMPILER_CLASSNAME + " in the classpath. Trying external compile...");
                return externCompile();
            }
            return internCompile();
        }
        return 0;
    }

    private int externCompile() {
        PrintStream savedOut = null;
        PrintStream savedErr = null;
        List<String> args = new ArrayList<String>();
        
        //prepare args
        args.add("javac");
        prepareArgs(args);
        
        if(captureOutput) {
            savedOut = System.out;
            savedErr = System.err;
            compileOutputStream = new ByteArrayOutputStream(DEFAULT_BYTE_ARRAY_BUFFER_SIZE);
            PrintStream out = new PrintStream(new BufferedOutputStream(compileOutputStream));
            System.setErr(out);
            System.setOut(out);
        }

        //compile
        int status;
        Runtime runtime = Runtime.getRuntime();
        try {
            status = runtime.exec(args.toArray(new String[args.size()])).waitFor();
        } catch (Throwable ex) {
            status = -1;
        }
        
        if(captureOutput) {
            System.setErr(savedErr);
            System.setOut(savedOut);
        }
        
        return status;
    }
    
    private int internCompile() {
        List<String> args = new ArrayList<String>();
        
        //prepare args
        prepareArgs(args);
        
        //compile
        int status;
        String[] stringArgs = args.toArray(new String[args.size()]);
        if(captureOutput) {
            compileOutputStream = new ByteArrayOutputStream(DEFAULT_BYTE_ARRAY_BUFFER_SIZE);
            PrintWriter writer = new PrintWriter(new BufferedOutputStream(compileOutputStream));
            status = com.sun.tools.javac.Main.compile(stringArgs, writer);
        } else {
            status = com.sun.tools.javac.Main.compile(stringArgs);
        }
        
        return status;
    }
    
    private void prepareArgs(List<String> args) {
        args.add("-d");
        args.add(destDirName);
        args.add(srcFileName);
        
        if(getClassPath().length() > 0) {
            args.add("-classpath");
            args.add(getClassPath());
        }
        
        if(!captureOutput) {
            args.add("-Xstdout");
            args.add(errFileName);
        }
        
        logger.debug("Compile arguments: " + StringUtils.stringArraytoString(args.toArray(new String[args.size()]), ' '));
    }
    
    private String getClassPath() {
        if(classPath == null) {
            classPath = ClassLoaderUtils.getClasspath(getClassLoader());
        }
        return classPath;
    }
    
    private boolean needRecompile() {
		if (forceRecompile) {
			return true;
		}

		if (destFile.exists()) {
			if (destFile.lastModified() >= srcFile.lastModified()) {
				return false; // is already compiled
			} else {
				final boolean deleted = destFile.delete();
				if (!deleted) {
					logger.error("error delete file " + destFile.getAbsolutePath());
				}
			}
		}

		// we need to recompile the source file
		return true;
	}
    
    public String getCapturedOutput() {
        if(captureOutput) {
            return (compileOutputStream == null) ? "" : compileOutputStream.toString();
        } else {
            return null;
        }
    }
    
    public boolean isForceRecompile() {
        return forceRecompile;
    }

    public void setForceRecompile(boolean forceRecompile) {
        this.forceRecompile = forceRecompile;
    }

    public String getErrFileName() {
        return errFileName;
    }

    public void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    public ClassLoader getClassLoader() {
        if(classLoader == null) {
            return getClass().getClassLoader();
        }
        return classLoader;
    }
}
