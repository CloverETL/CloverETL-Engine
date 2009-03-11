/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 *  Created on May 31, 2003
 */
package org.jetel.util;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.codehaus.janino.CompileException;
import org.jetel.test.CloverTestCase;
import org.jetel.util.compile.DynamicJavaCode;

/**
 * @author Wes Maciorowski; refactor Martin Varecha
 * @version 1.0
 */
public class CompileTest   extends CloverTestCase  {
	String testJavaFile1 = "src/org/jetel/userclasses/test1.java";	
	String testJavaClassFile1 = "org/jetel/userclasses/test1.class";	
	String testJavaFile2 = "src/org/jetel/userclasses/Main.java";	
	String testJavaClassFile2 = "org/jetel/userclasses/Main.class";	
	File classDirectory = new File("build/testspace");
	String src1 = null;
	String src2 = null;

	protected void setUp() {
		classDirectory.mkdirs();
		
		StringBuffer tmp = new StringBuffer();
		tmp.append("package org.jetel.userclasses;\n");
		tmp.append("public class test1 {\n");
		tmp.append("\tpublic static Integer addTwo(Integer i, Integer ii) {\n");
		tmp.append("\t\treturn new Integer(i.intValue()+ii.intValue());\n");
		tmp.append("\t}\n");
		tmp.append("}\n");
		src1 = tmp.toString();

		File aFile = new File(testJavaFile1);
		if (!aFile.exists()) {
			final File parentFile = new File(aFile.getParent());
			if (!parentFile.isDirectory()) {
				final boolean mkdirs = parentFile.mkdirs();
				assertTrue("can't create directory " + parentFile.getAbsolutePath(), mkdirs);
			}
			try {
				DataOutputStream fos = new DataOutputStream(new FileOutputStream(testJavaFile1));
				try {
					fos.writeBytes(tmp.toString());
				} finally {
					fos.close();
				}
			} catch (IOException e3) {
				e3.printStackTrace();
			}
		}

		tmp = new StringBuffer();
		tmp.append("public class Main {\n");
		tmp.append("public void method(Object s) {} \n");
		tmp.append("public void method(Long l) {} \n");
		tmp.append("public static void main(String[] args) {\n");
		tmp.append("	Main m = new Main();\n");
		tmp.append("	m.method(1==1 ? \"a\" : new Long(1)); \n");
		tmp.append("}\n");
		tmp.append("}\n");
		src2 = tmp.toString();

		aFile = new File(testJavaFile2);
		if (!aFile.exists()) {
			final File parentFile = new File(aFile.getParent());
			if (!parentFile.isDirectory()) {
				final boolean mkdirs = parentFile.mkdirs();
				assertTrue("can't create directory " + parentFile.getAbsolutePath(), mkdirs);
			}
			try {
				DataOutputStream fos = new DataOutputStream(new FileOutputStream(testJavaFile2));
				try {
					fos.writeBytes(tmp.toString());
				} finally {
					fos.close();
				}
			} catch (IOException e3) {
				e3.printStackTrace();
			}
		}
	}

	protected void tearDown() {
		//remove test Files if any
		File aFile=new File(testJavaFile1);
		 if(aFile.exists()) {
			 final boolean delete = aFile.delete();
			 assertTrue(delete);
		 }
		aFile=new File(testJavaClassFile1);
		 if(aFile.exists()) {
			 final boolean delete = aFile.delete();
			 assertTrue(delete);
		 }

		aFile=new File(testJavaFile2);
		 if(aFile.exists()) {
			 final boolean delete = aFile.delete();
			 assertTrue(delete);
		 }
		aFile=new File(testJavaClassFile2);
		 if(aFile.exists()) {
			 final boolean delete = aFile.delete();
			 assertTrue(delete);
		 }
		 
		aFile=new File(testJavaFile1);
		File parent = aFile.getParentFile();
			 if(parent.exists()) {
				 final boolean delete = parent.delete();
				 assertTrue(delete);
			 }
	}

	public static final int COMPILE_LOOPS = 5; 
	public void testCompilerJanino() {
		long start = System.currentTimeMillis();
		for (int i=0; i<COMPILE_LOOPS; i++){
			DynamicJavaCode djc = new DynamicJavaCode(src1);
			djc.instantiateByJanino();
		}
		long duration = System.currentTimeMillis() - start;
		System.out.println("janino compilation duration:"+duration);
	}

	public void testCompilerJanino2() {
		try {
			DynamicJavaCode djc = new DynamicJavaCode(src2);
			djc.instantiateByJanino();
			fail("shouldn't compile java 1.5 compatible code");
		} catch (Exception e){
			// OK
			if (!(e.getCause() instanceof CompileException)){
				fail("we expect CompileException;");
				e.printStackTrace();
			}
		}
	}

	/*
	public void testCompilerJanino3() {
		long start = System.currentTimeMillis();
		DynamicJavaCode djc = new DynamicJavaCode(src3);
		Object o = djc.instantiateByJanino();
		long duration = System.currentTimeMillis() - start;
		System.out.println("janino compilation3 duration:"+duration);
	}*/

	public void testCompilerJdkTools() {
		long start = System.currentTimeMillis();
		for (int i=0; i<COMPILE_LOOPS; i++){
			DynamicJavaCode djc = new DynamicJavaCode(src2);
			djc.instantiateByJdkTools();
		}
		long duration = System.currentTimeMillis() - start;
		System.out.println("jdkTools compilation duration:"+duration);
	}

	/**
	 *  Test for @link org.jetel.util.Compiler.CompilerClass(String className)
	 *
	 */
	public void testCompilerClass() {
		org.jetel.util.compile.Compiler compiler;

		compiler = new org.jetel.util.compile.Compiler(testJavaFile1,true,classDirectory.getAbsolutePath());
		compiler.compile();
		System.out.print(compiler.getCapturedOutput());
		final File class1File = new File(classDirectory,testJavaClassFile1);
		assertTrue(class1File.exists());

		compiler = new org.jetel.util.compile.Compiler(testJavaFile2,true,classDirectory.getAbsolutePath());
		compiler.compile();
		System.out.print(compiler.getCapturedOutput());
		assertFalse(new File(classDirectory,testJavaClassFile2).exists());
	}

}