/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002  David Pavlis
*
*    This program is free software; you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation; either version 2 of the License, or
*    (at your option) any later version.
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package org.jetel.util;

/**
 *  Helper class with some useful methods regarding file manipulation
 *
 * @author     dpavlis
 * @since    May 24, 2002
 */
public class FileUtils {


	/**
	 *  Translates fileURL into full path with all references to ENV variables resolved
	 *
	 * @param  fileURL  fileURL possibly containing references to ENV variables
	 * @return          The full path to file with ENV references resolved
	 * @since           May 24, 2002
	 */
	public static String getFullPath(String fileURL) {
		return fileURL;
	}


}

/*
* End class FileUtils
*/

