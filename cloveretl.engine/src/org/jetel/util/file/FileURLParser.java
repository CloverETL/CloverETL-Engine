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
package org.jetel.util.file;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * Helper class with some useful methods regarding file string manipulation
 *
 * @author Jan Ausperger (jan.ausperger@javlin.eu)
 *         (c) OpenSys (www.opensys.eu)
 */
public class FileURLParser {

	// for embedded source
	//     "something   :       (         something   )       [[#|/]something]?
	//      ([^:]*)     (:)     (\\()     (.*)        (\\))   (((#)(.*))|($))
	private final static Pattern INNER_SOURCE = Pattern.compile("(([^:]*)([:])([\\(]))(.*)(\\))((((#)|(//))(.*))|($))");

	/**
	 * Finds embedded source.
	 * 
	 * Example: 
	 * 		source:      zip:(http://linuxweb/~jausperger/employees.dat.zip)#employees0.dat
	 *      result: (g1) zip:
	 *              (g2) http://linuxweb/~jausperger/employees.dat.zip
	 *              (g3) #employees0.dat
	 * 
	 * @param source - input/output source
	 * @return matcher or null
	 */
	public static Matcher getInnerInput(String source) {
		Matcher matcher = INNER_SOURCE.matcher(source);
		return matcher.find() ? matcher : null;
	}
	
	/**
	 * Returns file name of input (URL).
	 * 
	 * @param input - url file name
	 * @return file name
	 * @throws MalformedURLException
	 * 
	 * @see java.net.URL#getFile()
	 */
	public static String getFile(URL contextURL, String input) throws MalformedURLException {
		Matcher matcher = getInnerInput(input);
		String innerSource2, innerSource3;
		if (matcher != null && (innerSource2 = matcher.group(5)) != null) {
			if (!(innerSource3 = matcher.group(7)).equals("")) {
				return innerSource3.substring(1);
			} else {
				input = getFile(null, innerSource2);
			}
		}
		URL url = FileUtils.getFileURL(contextURL, input);
		if (url.getRef() != null) return url.getRef();
		else {
			input = url.getFile();
			if (input.startsWith("zip:") || input.startsWith("tar:")) {
				input = input.contains("#") ? 
					input.substring(input.lastIndexOf('#') + 1) : 
					input.substring(input.indexOf(':') + 1);
			} else if (input.startsWith("gzip:")) {
				input = input.substring(input.indexOf(':') + 1);
			}
			return input;
		}
	}

	/**
	 * Adds final slash to the directory path, if it is necessary.
	 * @param directoryPath
	 * @return
	 */
	public static String appendSlash(String directoryPath) {
		if(directoryPath.length() == 0 || directoryPath.endsWith("/") || directoryPath.endsWith("\\")) {
			return directoryPath;
		} else {
			return directoryPath + "/";
		}
	}
	
	/**
	 * Gets the most inner url address.
	 * @param contextURL 
	 * 
	 * @param input
	 * @return
	 * @throws IOException
	 */
	public static String getMostInnerAddress(String input) {
        // get inner input
        String innerInput = getInnerAddress(input);
        return innerInput == null ? input : getMostInnerAddress(innerInput);
	}
	
	/**
	 * Gets inner url address. 
	 * @param input
	 * @return
	 */
	public static String getInnerAddress(String input) {
		if (input == null) return null;
		
		// get inner source
		Matcher matcher = getInnerInput(input);
		String innerSource;
		if (matcher != null && (innerSource = matcher.group(5)) != null) {
			return innerSource;
		}
		
        //resolve url format for zip files
        if(input.startsWith("zip:") || input.startsWith("tar:")) {
            if(!input.contains("#")) { //url is given without anchor - later is returned channel from first zip entry 
            	input = input.substring(input.indexOf(':') + 1);
            } else {
                input = input.substring(input.indexOf(':') + 1, input.lastIndexOf('#'));
            }
            return input;
        }
        else if (input.startsWith("gzip:")) {
        	return input.substring(input.indexOf(':') + 1);
        }
        
        return null;
	}
	
}
