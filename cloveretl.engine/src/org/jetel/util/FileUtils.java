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
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.zip.Adler32;
import java.util.zip.Checksum;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.jetel.data.Defaults;
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
     * Creates URL object based on specified fileURL string. Handles
     * situations when <code>fileURL</code> contains only path to file
     * <i>(without "file:" string)</i>. 
     * 
     * @param fileURL   string containing file URL
     * @return  URL object or NULL if object can't be created (due to Malformed URL)
     */
    public static URL getFileURL(String fileURL){
        URL url;
        try{
            url = new URL(fileURL);
        }catch(MalformedURLException ex){
            try{
                url = new URL("file:"+fileURL);
            }catch(MalformedURLException ex1){
                return null;
            }
        }
        return url;
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
	
    /**
     * Creates ReadableByteChannel from the url definition.
     * <p>All standard url format are acceptable plus extended form of url by zip & gzip construction:</p>
     * Example: zip:&lt;url_to_file&gt;#&lt;inzip_path_to_file&gt; <br>
     * Example: gzip:&lt;url_to_file&gt; <br>
     * 
     * @param input URL of file to read
     * @return
     * @throws IOException
     */
    public static ReadableByteChannel getReadableChannel(String input) throws IOException {
        String strURL = input;
        URL url;
        
        //resolve url format for zip files
        if(input.startsWith("zip:")) {
            strURL = input.substring(input.indexOf(':') + 1, input.lastIndexOf('#'));
        }else if (input.startsWith("gzip:")) {
            strURL = input.substring(input.indexOf(':') + 1);
        }
        
        //open channel
        try {
            url = new URL(strURL); 
        } catch(MalformedURLException e) {
            // try to patch the url
            try {
                url = new URL("file:" + strURL);
            } catch(MalformedURLException ex) {
                throw new RuntimeException("Wrong URL of file specified: " + ex.getMessage());
            }
        }

        //resolve url format for zip files
        if(input.startsWith("zip:")) {
            String zipAnchor = input.substring(input.lastIndexOf('#') + 1);
            ZipInputStream zin = new ZipInputStream(url.openStream()) ;     
            ZipEntry entry;
            while((entry = zin.getNextEntry()) != null) {
                if(entry.getName().equals(zipAnchor)) {
                    return Channels.newChannel(zin);
                }
                //finish up with entry
                zin.closeEntry();
            }
            //close the archive
            zin.close();
            throw new RuntimeException("Wrong anchor (" + zipAnchor + ") to zip file.");
        }else if (input.startsWith("gzip:")) {
            GZIPInputStream gzin = new GZIPInputStream(url.openStream(),Defaults.DEFAULT_IOSTREAM_CHANNEL_BUFFER_SIZE);
            return Channels.newChannel(gzin);
        }
        
        return Channels.newChannel(url.openStream());
    }

}

/*
 *  End class FileUtils
 */

