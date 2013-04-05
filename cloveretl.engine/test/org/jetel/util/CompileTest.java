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

import org.jetel.exception.LoadClassException;
import org.jetel.test.CloverTestCase;
import org.jetel.util.compile.CompilationException;
import org.jetel.util.compile.DynamicCompiler;
import org.jetel.util.compile.DynamicJavaClass;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * @author Wes Maciorowski; refactor Martin Varecha
 * @version 1.0
 */
public class CompileTest extends CloverTestCase {

	private static final int COMPILE_LOOPS = 5; 

	private String src1;
	private String src2;

	@Override
	@SuppressWarnings(value = "NP_LOAD_OF_KNOWN_NULL_VALUE")
	protected void setUp() {
		StringBuffer tmp = new StringBuffer();
		tmp.append("package org.jetel.userclasses;\n");
		tmp.append("public class test1 {\n");
		tmp.append("\tpublic static Integer addTwo(Integer i, Integer ii) {\n");
		tmp.append("\t\treturn new Integer(i.intValue()+ii.intValue());\n");
		tmp.append("\t}\n");
		tmp.append("}\n");
		src1 = tmp.toString();

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
	}

	public void testDynamicCompiler() {
		long start = System.currentTimeMillis();
		DynamicCompiler compiler = new DynamicCompiler(getClass().getClassLoader());
		for (int i=0; i<COMPILE_LOOPS; i++){
			try {
				compiler.compile(src1, "org.jetel.userclasses.test1");
			} catch (CompilationException exception) {
				exception.printStackTrace();
				System.out.println(exception.getCompilerOutput());
				fail("Compilation failed!");
			}
		}
		long duration = System.currentTimeMillis() - start;
		System.out.println("Java compilation duration:"+duration);
	}
	
	public void testDynamicJavaClass() {
		long start = System.currentTimeMillis();
		for (int i=0; i<COMPILE_LOOPS; i++){
			try {
				DynamicJavaClass.instantiate(src2, getClass().getClassLoader());
			} catch (LoadClassException exception) {
				exception.printStackTrace();
				fail("Compilation failed!");
			}
		}
		long duration = System.currentTimeMillis() - start;
		System.out.println("Java compilation duration:"+duration);
	}

}