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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.zip.Adler32;
import java.util.zip.Checksum;
/**
 *  Helper class with some useful methods regarding file manipulation
 *
 * @author      dpavlis
 * @since       May 24, 2002
 * @revision    $Revision$
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


	/**
	 *  Calculates checksum of specified file.<br>Is suitable for short files (not very much buffered).
	 *
	 * @param  filename  Filename (full path) of file to calculate checksum for
	 * @return           Calculated checksum or -1 if some error (IO) occured
	 */
	public static long calculateFileCheckSum(String filename) {
		byte[] buffer = new byte[1024];
		Checksum checksum = new Adler32(); // we use Adler - should be faster
		int length = 0;
		try {
			FileInputStream dataFile = new FileInputStream(filename);
			while (length != -1) {
				length = dataFile.read(buffer);
				if (length > 0) {
					checksum.update(buffer, 0, length);
				}
			}
			dataFile.close();

		} catch (IOException ex) {
			return -1;
		}
		return checksum.getValue();
	}

	/**
	 * Reads file specified by URL. The content is returned as String
	 * @param fileURL URL specifying the location of file from which to read
	 * @return
	 */
	public static String getStringFromURL(String fileURL){
	    String string = null;
        URL url;
		try{
			url = new URL(fileURL); 
		}catch(MalformedURLException e){
			// try to patch the url
			try {
				url=new URL("file:"+fileURL);
			}catch(MalformedURLException ex){
				throw new RuntimeException("Wrong URL of file specified: "+ex.getMessage());
			}
		}

		StringBuffer sb = new StringBuffer(2048);
        
		try
        {
            char[] charBuf=new char[256];
            BufferedReader in=new BufferedReader(new InputStreamReader(url.openStream()));
            int readNum;
            
            while ((readNum=in.read(charBuf,0,charBuf.length)) != -1)
            {
                sb.append(charBuf,0,readNum);
            }
        }
        catch(IOException ex)
        {
            throw new RuntimeException("Can't get string from file " + fileURL + " : " + ex.getMessage());
        }
        return sb.toString();
	}
	
}

/*
 *  End class FileUtils
 */

