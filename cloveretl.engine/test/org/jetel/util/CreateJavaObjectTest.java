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
package test.org.jetel.util;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.jetel.exception.ClassCompilationException;
import org.jetel.util.Compile;
import org.jetel.util.CreateJavaObject;

import junit.framework.TestCase;

/**
 * @author Wes Maciorowski
 * @version 1.0
 * 
 * JUnit tests for org.jetel.util.Compile class.
 */
public class CreateJavaObjectTest    extends TestCase  {
	String testJavaFile1 = "src\\org\\jetel\\userclasses\\test1.java";	
	String testJavaClassFile1 = "org\\jetel\\userclasses\\test1.class";	
	String testJavaFile2 = "src\\org\\jetel\\userclasses\\test2.java";	
	String testJavaClassFile2 = "org\\jetel\\userclasses\\test2.class";	
	String classDirectory = "bin";
//	private org.jetel.userclasses.test1 t1 = null;
//
//	private org.jetel.userclasses.test2 t2 = null;

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

//	try {
//		Compile.compileClass(testJavaFile1,classDirectory);
//	} catch (ClassCompilationException e) {
//		e.printStackTrace();
//	}


	tmp = new StringBuffer();
	tmp.append("package org.jetel.userclasses;\n");	
	tmp.append("public class test2 {\n");
	tmp.append("\tInteger i;\n");
	tmp.append("\tInteger ii;\n");
	tmp.append("\tpublic test2(Integer i, Integer ii) {\n");
	tmp.append("\tthis.i = i;\n");
	tmp.append("\tthis.ii = ii;\n");
	tmp.append("\t}\n");
	tmp.append("\tpublic Integer addTwo() {\n");
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

//	try {
//		//Compile.compileClass(testJavaFile2,classDirectory);
//	} catch (ClassCompilationException e) {
//		e.printStackTrace();
//	}

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
 *  Test for @link org.jetel.util.Compile.compileClass(String className)
 *
 */
public void test_createObject() throws Exception     {
//	t1 = (org.jetel.userclasses.test1) CreateJavaObject.createObject("org.jetel.userclasses.test1");
//	assertEquals(t1 != null, true);
//	assertEquals(t1.addTwo(new Integer(7),new Integer(17)).intValue(),24);
//	Object[] anObj = null;
//	anObj = new Object[2];
//	anObj[0] = new Integer(7);
//	anObj[1] = new Integer(7);
//
//	t2 = (org.jetel.userclasses.test2) CreateJavaObject.createObject("org.jetel.userclasses.test2",anObj);
//	assertEquals(t2.addTwo().intValue(),14);
}

public void test_invokeMethod() throws Exception     {

	Object[] anObj = null;
	anObj = new Object[2];
	anObj[0] = new Integer(7);
	anObj[1] = new Integer(7);

	Integer tmpInteger = (Integer) CreateJavaObject.invokeMethod("org.jetel.userclasses.test2","addTwo",anObj, null);
	assertEquals(tmpInteger.intValue(),14);

	anObj[0] = new Integer(17);
	tmpInteger = (Integer) CreateJavaObject.invokeMethod("org.jetel.userclasses.test1","addTwo", null,anObj);
	assertEquals(tmpInteger.intValue(),24);
}
}
