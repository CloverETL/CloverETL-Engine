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

import java.io.File;
import org.jetel.exception.*;
import com.sun.tools.javac.Main;

/**
 * @author      Wes Maciorowski, David Pavlis
 * @since
 * @revision    $Revision$
 */
public class Compile {

	private final static String compilerClassname = "sun.tools.javac.Main";
	//private static final constructorSignature = new Class[] { OutputStream.class, String.class };

	/*
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
	/**
	 *  This method compiles specified class
	 *
	 * @param  className                      The name of the class (without .java)
	 * @param  classDirectory                 The directory where to find souce code of the class and where
	 *					  to put resulting compiled class
	 * @return                                True if success, otherwise ClassCompilationException is raised
	 * @exception  ClassCompilationException  Exception which indicates that something went wrong !
	 */
	public static boolean compileClass(String className, String classDirectory)
			 throws ClassCompilationException {

		String[] args = new String[]{"-d", classDirectory, classDirectory + className + ".java"
						, "-Xstdout" , classDirectory + className + ".err" };

		//debug
		// for(int i=0;i<args.length;System.out.println(args[i++]));

		int status = com.sun.tools.javac.Main.compile(args);

		if (status != 0) {
			throw new ClassCompilationException(classDirectory+className);
		}

		return true;
	}

}

