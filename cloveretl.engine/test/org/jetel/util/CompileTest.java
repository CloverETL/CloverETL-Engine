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

import org.jetel.exception.ClassCompilationException;

import junit.framework.TestCase;

/**
 * @author Wes Maciorowski
 * @version 1.0
 * 
 * JUnit tests for org.jetel.util.Compiler class.
 */
public class CompileTest   extends TestCase  {
	String testJavaFile1 = "src/org/jetel/userclasses/test1.java";	
	String testJavaClassFile1 = "org/jetel/userclasses/test1.class";	
	String testJavaFile2 = "src/org/jetel/userclasses/test2.java";	
	String testJavaClassFile2 = "org/jetel/userclasses/test2.class";	
	String classDirectory = "bin";

	protected void setUp() {
		StringBuffer tmp = new StringBuffer();
		tmp.append("package org.jetel.userclasses;\n");	
		tmp.append("public class test1 {\n");
		tmp.append("\tpublic static Integer addTwo(Integer i, Integer ii) {\n");
		tmp.append("\t\treturn new Integer(i.intValue()+ii.intValue());\n");
		tmp.append("\t}\n");
		tmp.append("}\n");

		File aFile=new File(testJavaFile1);
		 if(!aFile.exists()) {
			new File(aFile.getParent()).mkdirs();
			try {
				DataOutputStream fos = new DataOutputStream(new FileOutputStream(testJavaFile1));
				fos.writeBytes(tmp.toString());
			} catch (IOException e3) {
				e3.printStackTrace();
			}
		 }


		tmp = new StringBuffer();
		tmp.append("package org.jetel.userclasses;\n");	
		tmp.append("public classes test1 {\n");
		tmp.append("\tpublic static Integer addTwo(Integer i, Integer ii) {\n");
		tmp.append("\t\treturn new Integer(i.intValue()+ii.intValue());\n");
		tmp.append("\t}\n");
		tmp.append("}\n");

		aFile=new File(testJavaFile2);
		 if(!aFile.exists()) {
			new File(aFile.getParent()).mkdirs();
			try {
				DataOutputStream fos = new DataOutputStream(new FileOutputStream(testJavaFile2));
				fos.writeBytes(tmp.toString());
			} catch (IOException e3) {
				e3.printStackTrace();
			}
		 }
	}

protected void tearDown() {
	//remove test Files if any
	File aFile=new File(testJavaFile1);
	 if(aFile.exists()) {
		 aFile.delete();
	 }
	aFile=new File(testJavaClassFile1);
	 if(aFile.exists()) {
		 aFile.delete();
	 }

	aFile=new File(testJavaFile2);
	 if(aFile.exists()) {
		 aFile.delete();
	 }
	aFile=new File(testJavaClassFile2);
	 if(aFile.exists()) {
		 aFile.delete();
	 }
}

/**
 *  Test for @link org.jetel.util.Compiler.CompilerClass(String className)
 *
 */

public void testCompilerClass() {
	org.jetel.util.compile.Compiler compiler;

	compiler = new org.jetel.util.compile.Compiler(testJavaFile1,true,classDirectory);
	compiler.compile();
	assertTrue(new File(classDirectory+File.separator+testJavaClassFile1).exists());

	compiler = new org.jetel.util.compile.Compiler(testJavaFile2,true,classDirectory);
	compiler.compile();
	System.out.print(compiler.getCapturedOutput());
	assertFalse(new File(classDirectory+File.separator+testJavaClassFile2).exists());
}

}