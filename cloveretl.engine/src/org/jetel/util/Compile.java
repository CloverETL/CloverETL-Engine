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

/**
 * @author Wes Maciorowski
 *
 */
public class Compile {

	public static boolean compileClass(String className, String classDirectory)
		throws ClassCompilationException {

		String tmpLocation = System.getProperty("user.dir");
		String[] args = new String[] { "-d", tmpLocation +File.separator+classDirectory, className };

		int status = com.sun.tools.javac.Main.compile(args);

		if (status != 0)
			throw new ClassCompilationException(className);

		return true;
	}

}
