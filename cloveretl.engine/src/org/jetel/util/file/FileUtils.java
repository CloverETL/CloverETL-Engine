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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.Adler32;
import java.util.zip.Checksum;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.MultiOutFile;
import org.jetel.util.bytes.SystemOutByteChannel;
import org.jetel.util.protocols.ftp.FTPStreamHandler;
import org.jetel.util.protocols.sftp.SFTPConnection;
import org.jetel.util.protocols.sftp.SFTPStreamHandler;

import sun.misc.BASE64Encoder;

import com.jcraft.jsch.ChannelSftp;
/**
 *  Helper class with some useful methods regarding file manipulation
 *
 * @author      dpavlis
 * @since       May 24, 2002
 * @revision    $Revision$
 */
public class FileUtils {

	// for embedded source
	//     "something   :       (         something   )       [#something]?
	//      ([^:]*)     (:)     (\\()     (.*)        (\\))   (((#)(.*))|($))
	private final static Pattern INNER_SOURCE = Pattern.compile("(([^:]*)([:])([\\(]))(.*)(\\))(((#)(.*))|($))");

	// standard input/output source
	private final static String STD_SOURCE = "-";
	
	// sftp protocol handler
	public static final SFTPStreamHandler sFtpStreamHandler = new SFTPStreamHandler();

	// ftp protocol handler
	public static final FTPStreamHandler ftpStreamHandler = new FTPStreamHandler();

	// file protocol name
	private static final String FILE_PROTOCOL = "file";
	
    public static URL getFileURL(String fileURL) throws MalformedURLException {
    	return getFileURL((URL) null, fileURL);
    }

    public static URL getFileURL(String contextURL, String fileURL) throws MalformedURLException {
    	return getFileURL(getFileURL((URL) null, contextURL), fileURL);
    }
    
    /**
     * Creates URL object based on specified fileURL string. Handles
     * situations when <code>fileURL</code> contains only path to file
     * <i>(without "file:" string)</i>. 
     * 
     * @param contextURL context URL for converting relative to absolute path (see TransformationGraph.getProjectURL()) 
     * @param fileURL   string containing file URL
     * @return  URL object or NULL if object can't be created (due to Malformed URL)
     * @throws MalformedURLException  
     */
    public static URL getFileURL(URL contextURL, String fileURL) throws MalformedURLException {
        try {
            return new URL(contextURL, fileURL);
        } catch(MalformedURLException ex) {
        	try {
            	// ftp connection is connected via sftp handler, 22 port is ok but 21 is somehow blocked for the same connection
        		return new URL(contextURL, fileURL, sFtpStreamHandler);
            } catch(Exception e) {
                return new URL(contextURL, "file:" + fileURL);
            }
        }
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
     * @param contextURL context URL for converting relative to absolute path (see TransformationGraph.getProjectURL()) 
	 * @param fileURL URL specifying the location of file from which to read
	 * @return
	 */
	public static String getStringFromURL(URL contextURL, String fileURL, String charset){
        URL url;
        String chSet = charset != null ? charset : Defaults.DataParser.DEFAULT_CHARSET_DECODER;
		try {
			url = FileUtils.getFileURL(contextURL, fileURL);
		}catch(MalformedURLException ex){
			throw new RuntimeException("Wrong URL of file specified: "+ex.getMessage());
		}

		StringBuffer sb = new StringBuffer(2048);
		try {
            char[] charBuf=new char[256];
            BufferedReader in=new BufferedReader(new InputStreamReader(url.openStream(), chSet));
            int readNum;
            
            while ((readNum=in.read(charBuf,0,charBuf.length)) != -1) {
                sb.append(charBuf,0,readNum);
            }
        } catch(IOException ex) {
            throw new RuntimeException("Can't get string from file " + fileURL + " : " + ex.getMessage());
        }
        return sb.toString();
	}
	
    /**
     * Creates ReadableByteChannel from the url definition.
     * <p>All standard url format are acceptable plus extended form of url by zip & gzip construction:</p>
     * Examples:
     * <dl>
     *  <dd>zip:&lt;url_to_zip_file&gt;#&lt;inzip_path_to_file&gt;</dd>
     *  <dd>gzip:&lt;url_to_gzip_file&gt;</dd>
     * </dl>
     * @param contextURL context URL for converting relative to absolute path (see TransformationGraph.getProjectURL()) 
     * @param input URL of file to read
     * @return
     * @throws IOException
     */
    public static ReadableByteChannel getReadableChannel(URL contextURL, String input) throws IOException {
        String zipAnchor = null;
        URL url = null;
        InputStream innerStream = null;
		boolean isZip = false;
		boolean isGzip = false;
        
		// get inner source
		Matcher matcher = getInnerInput(input);
		String innerSource;
		if (matcher != null && (innerSource = matcher.group(5)) != null) {
			innerStream = Channels.newInputStream(getReadableChannel(null, innerSource));
			input = matcher.group(2) + matcher.group(3) + matcher.group(7);
		}
		
		// std input (console)
		if (input.equals(STD_SOURCE)) {
			return Channels.newChannel(System.in);
		}
		
        //resolve url format for zip files
        if(input.startsWith("zip:")) {
        	isZip = true;
            if(!input.contains("#")) { //url is given without anchor - later is returned channel from first zip entry 
                zipAnchor = null;
            	input = input.substring(input.indexOf(':') + 1);
            } else {
                zipAnchor = input.substring(input.lastIndexOf('#') + 1);
                input = input.substring(input.indexOf(':') + 1, input.lastIndexOf('#'));
            }
        }
        else if (input.startsWith("gzip:")) {
        	isGzip = true;
        	input = input.substring(input.indexOf(':') + 1);
        }
        
        //open channel
        if (innerStream == null) {
        	url = FileUtils.getFileURL(contextURL, input);
        	innerStream = getAuthorizedStream(url);
        }

        if (isZip) {
            //resolve url format for zip files
            ZipInputStream zin = new ZipInputStream(innerStream) ;     
            ZipEntry entry;
            while((entry = zin.getNextEntry()) != null) {
                if(zipAnchor == null) { //url is given without anchor; first entry in zip file is used
                    return Channels.newChannel(zin);
                }
                if(entry.getName().equals(zipAnchor)) {
                    return Channels.newChannel(zin);
                }
                //finish up with entry
                zin.closeEntry();
            }
            //close the archive
            zin.close();
            //no channel found report
            if(zipAnchor == null) {
                throw new IOException("Zip file is empty.");
            } else {
                throw new IOException("Wrong anchor (" + zipAnchor + ") to zip file.");
            }
        } 
        
        // gzip
        else if (isGzip) {
            GZIPInputStream gzin = new GZIPInputStream(innerStream, Defaults.DEFAULT_IOSTREAM_CHANNEL_BUFFER_SIZE);
            return Channels.newChannel(gzin);
        }
        
        return Channels.newChannel(innerStream);
    }

    private static InputStream getAuthorizedStream(URL url) throws IOException {
        URLConnection uc = url.openConnection();
        // check autorization
        if (url.getUserInfo() != null) {
            uc.setRequestProperty("Authorization", "Basic " + encode(url.getUserInfo()));
        }
        return uc.getInputStream();
    }

    private static String encode (String source) {
    	  BASE64Encoder enc = new sun.misc.BASE64Encoder();
    	  return enc.encode(source.getBytes());
   	 }
    
	/**
     * Creates WritableByteChannel from the url definition.
     * <p>All standard url format are acceptable (including ftp://) plus extended form of url by zip & gzip construction:</p>
     * Examples:
     * <dl>
     *  <dd>zip:&lt;url_to_zip_file&gt;#&lt;inzip_path_to_file&gt;</dd>
     *  <dd>gzip:&lt;url_to_gzip_file&gt;</dd>
     * </dl>
     * @param contextURL context URL for converting relative to absolute path (see TransformationGraph.getProjectURL()) 
	 * @param input
	 * @param appendData
	 * @return
	 * @throws IOException
	 */
	public static WritableByteChannel getWritableChannel(URL contextURL, String input, boolean appendData) throws IOException {
        String zipAnchor = null;
		boolean isZip = false;
		boolean isGzip = false;
		boolean isFtp = false;
		
		// std input (console)
		if (input.equals(STD_SOURCE)) {
			return new SystemOutByteChannel();
		}
		
		// prepare string/path to output file
		if(input.startsWith("zip:")) {
	        //resolve url format for zip files
			isZip = true;
            if(input.contains("#")) {
                zipAnchor = input.substring(input.lastIndexOf('#') + 1);
                input = input.substring(input.indexOf(':') + 1, input.lastIndexOf('#'));
            } else {
            	input = input.substring(input.indexOf(':') + 1);
                zipAnchor = "default_output";
            }
        } else if(input.startsWith("gzip:")) {
            //resolve url format for gzip files
    		isGzip = true;
    		input = input.substring(5);
        } else if (input.startsWith("ftp") || input.startsWith("sftp") || input.startsWith("scp")) {
        	isFtp = true;
        }

		// create output stream
        OutputStream os = null;
		if(isFtp) {
			// ftp output stream
			URL url = FileUtils.getFileURL(contextURL, input);
			URLConnection urlConnection = url.openConnection();
			if (urlConnection instanceof SFTPConnection) ((SFTPConnection)urlConnection).setMode(appendData ? ChannelSftp.APPEND : ChannelSftp.OVERWRITE);
			os = urlConnection.getOutputStream();
		} else {
			// file input stream 
			String filePath = FileUtils.getFile(contextURL, input);
			os = new FileOutputStream(filePath, appendData);
		}

		// create writable channel
		// zip channel
		if(isZip) {
			// resolve url format for zip files
			ZipOutputStream zout = new ZipOutputStream(os);
			ZipEntry entry = new ZipEntry(zipAnchor);
			zout.putNextEntry(entry);
			return Channels.newChannel(zout);
        } 
		
		// gzip channel
		else if (isGzip) {
            GZIPOutputStream gzos = new GZIPOutputStream(os, Defaults.DEFAULT_IOSTREAM_CHANNEL_BUFFER_SIZE);
            return Channels.newChannel(gzos);
        } 
		
		// other channel
       	return Channels.newChannel(os);
	}
    
	/**
	 * This method checks whether is is possible to write to given file
	 * 
	 * @param contextURL
	 * @param fileURL
	 * @return true if can write, false otherwise
	 * @throws ComponentNotReadyException
	 */
	public static boolean canWrite(URL contextURL, String fileURL)
			throws ComponentNotReadyException {
		String fileName;
		if (fileURL.startsWith("zip:")){
			fileName = fileURL.substring(fileURL.indexOf(':') + 1, 
					fileURL.indexOf('#'));
		}else if (fileURL.startsWith("gzip:")){
			fileName = fileURL.substring(fileURL.indexOf(':') + 1);
		}else{
			fileName = fileURL;
		}
		MultiOutFile multiOut = new MultiOutFile(fileName);
		File file;
        URL url;
		boolean tmp;
		//create file on given URL
		try {
			url = getFileURL(contextURL, multiOut.next());
            if(!url.getProtocol().equalsIgnoreCase("file")) return true;
			file = new File(url.getPath());
		} catch (Exception e) {
			throw new ComponentNotReadyException(e + ": " + fileURL);
		}
		//check if can write to this file
		if (file.exists()) {
			tmp = file.canWrite();
		} else {
			try {
				tmp = file.createNewFile();
			} catch (IOException e) {
				throw new ComponentNotReadyException(e + ": " + fileURL);
			}
			if (tmp) {
				file.delete();
			}
		}
		if (!tmp) {
			throw new ComponentNotReadyException("Can't write to: " + fileURL);
		}
		return true;
	}
	
	/**
	 * This method checks whether is is possible to write to given directory
	 * 
	 * @param dest
	 * @return true if can write, false otherwise
	 */
	public static boolean canWrite(File dest){
		if (dest.exists()) return dest.canWrite();
		
		List<File> dirs = getDirs(dest);
		
		boolean result = dest.mkdirs();
				
		//clean up
		for (File dir : dirs) {
			if (dir.exists()) dir.delete();
		}
		
		return result;
	}
	
	/**
	 * @param file
	 * @return list of directories, which have to be created to create this directory (don't exist),
	 * 	  empty list if requested directory exists
	 */
	private static List<File> getDirs(File file){
		ArrayList<File> dirs = new ArrayList<File>();
		File dir = file;
		if (file.exists()) return dirs;
		dirs.add(file);
		while (!(dir = dir.getParentFile()).exists()) {
			dirs.add(dir);
		}
		return dirs;
	}

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
		URL url = getFileURL(contextURL, input);
		if (url.getRef() != null) return url.getRef();
		else {
			input = url.getFile();
			if (input.startsWith("zip:")) {
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
		if(directoryPath.endsWith("/") || directoryPath.endsWith("\\")) {
			return directoryPath;
		} else {
			return directoryPath + "/";
		}
	}
	
	/**
	 * Parses address and returns true if the address contains a server.
	 * 
	 * @param input
	 * @return
	 * @throws IOException
	 */
	public static boolean isServerURL(URL url) throws IOException {
		return url != null && !url.getProtocol().equals(FILE_PROTOCOL);
	}
	
	/**
	 * Creates URL connection and connect the server.
	 * 
	 * @param url
	 * @throws IOException
	 */
	public static void checkServer(URL url) throws IOException {
		if (url == null) {
			return;
		}
		URLConnection connection = url.openConnection();
		connection.connect();
		if (connection instanceof SFTPConnection) {
			((SFTPConnection) connection).disconnect();
		}
	}

	/**
	 * Gets the most inner url address.
	 * 
	 * @param input
	 * @return
	 * @throws IOException
	 */
	public static URL getInnerAddress(String input) throws IOException {
        URL url = null;
        
		// get inner source
		Matcher matcher = getInnerInput(input);
		String innerSource;
		if (matcher != null && (innerSource = matcher.group(2)) != null) {
			url = getInnerAddress(innerSource);
			input = matcher.group(1) + matcher.group(3);
		}
		
		// std input (console)
		if (input.equals(STD_SOURCE)) {
			return null;
		}
		
        //resolve url format for zip files
        if(input.startsWith("zip:")) {
            if(!input.contains("#")) { //url is given without anchor - later is returned channel from first zip entry 
            	input = input.substring(input.indexOf(':') + 1);
            } else {
                input = input.substring(input.indexOf(':') + 1, input.lastIndexOf('#'));
            }
        }
        else if (input.startsWith("gzip:")) {
        	input = input.substring(input.indexOf(':') + 1);
        }
        
        return url == null ? FileUtils.getFileURL(input) : url;
	}
	
}

/*
 *  End class FileUtils
 */

