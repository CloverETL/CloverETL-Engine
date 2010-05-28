/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.util.file;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.commons.net.ftp.FTPFile;
import org.jetel.data.Defaults;
import org.jetel.enums.ArchiveType;
import org.jetel.util.protocols.ftp.FTPConnection;
import org.jetel.util.protocols.proxy.ProxyHandler;
import org.jetel.util.protocols.sftp.SFTPConnection;

import com.jcraft.jsch.ChannelSftp.LsEntry;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 09/05/06  
 * Class generating collection of filenames which match given wildcard patterns.
 * The pattern may describe either relative or absolute filenames.
 * '*' represents any count of characters, '?' represents one character.
 * Wildcards cannot be followed by directory separators. 
 */
public class WcardPattern {

	// for embedded source
	//     "[zip|gzip|tar]     :       ^(       something   )          [[#|$]something]?
	//      ((zip|gzip|tar)    :       ([^\\(]  .*          [^\\)])    #(.*))|((zip|gzip|tar):([^\\(].*[^\\)])$)
	private final static Pattern ARCHIVE_SOURCE = Pattern.compile("((zip|gzip|tar):([^\\(].*[^\\)])#(.*))|((zip|gzip|tar):([^\\(].*[^\\)])$)");
	
	// Wildcard characters.
	@SuppressWarnings("MS")
	public final static char[] WCARD_CHAR = {'*', '?'};

	// Regex substitutions for wildcards. 
	private final static String[] REGEX_SUBST = {".*", "."};

	// Start sequence for regex quoting.
	private final static String REGEX_START_QUOTE = "\\Q";

	// End sequence for regex quoting
	private final static String REGEX_END_QUOTE = "\\E";

	// Regex start anchor.
	private final static char REGEX_START_ANCHOR = '^';

	// Regex end anchor.
	private final static char REGEX_END_ANCHOR = '$';

	// file protocol
	private final static String FILE = "file";
	
	// sftp protocol
	private final static String SFTP = "sftp";
	
	// ftp protocol
	private final static String FTP = "ftp";

	// Collection of filename patterns.
	private List<String> patterns;
	
	// Context URL.
	private URL parent;

	// default - resolve server names
	private boolean resolveAllNames = true;
	
	/**
	 * Constructor. Creates instance with empty set of patterns. It doesn't match any filenames initially.  
	 */
	public WcardPattern() {
		patterns = new ArrayList<String>(1);
	}
	
	/**
	 * Adds filename pattern.
	 * @param pat Pattern to be added.
	 */
	public void addPattern(String pat) {
		patterns.add(pat);
	}

	/**
	 * Add filename patterns.
	 * @param pat Array of patterns
	 */
	public void addPattern(String[] pat) {
		for (int i = 0; i < pat.length; i++) {
			addPattern(pat[i]);
		}
	}

	/**
	 * Add filename patterns.
	 * @param pat Pattern list
	 * @param sep Pattern separator
	 */
	public void addPattern(String pat, String sep) {
		addPattern(pat.split(sep));
	}

	/**
	 * Splits specified pattern in two parts - directory which cannot contain any wildcard
	 * and filename pattern containing wildcards. When specified pattern doesn't contain
	 * any wildcards, doesn't do anything. 
	 * @param pat Pattern to be split.
	 * @param dir Directory name.
	 * @param filePat Filename pattern. 
	 * @return false for pattern without wildcards, true otherwise. 
	 */
	private boolean splitFilePattern(String pat, StringBuffer dir, StringBuffer filePat) {
		dir.setLength(0);
		filePat.setLength(0);

		File f = new File(pat);
		dir.append(f.getParent());
		filePat.append(f.getName());

		for (int wcardIdx = 0; wcardIdx < WCARD_CHAR.length; wcardIdx++) {
			if (filePat.indexOf("" + WCARD_CHAR[wcardIdx]) >= 0) { // wildcard found
				return true;
			}
		}

		// no wildcard in pattern
		return false;
	}
	
	/**
	 * Sets if the method filenames resolves server paths. 
	 * @param resolveAllNames
	 * @throws IOException
	 */
	public void resolveAllNames(boolean resolveAllNames) {
		this.resolveAllNames = resolveAllNames;
	}

	/**
	 * Generates filenames matching current set of patterns.
	 * @return Set of matching filenames.
	 * @throws IOException 
	 */
	public List<String> filenames() throws IOException {
		List<String> mfiles = new ArrayList<String>();
		String fileName;
		
		// goes through filenames separated by ';'
		for (int i = 0; i < patterns.size(); i++) {
			fileName = patterns.get(i);
			// returns list of names for filename that can have a wild card '?' or '*'
			for (FileStreamName fileStreamName: innerFileNames(fileName, false)) {
				if (fileStreamName.getInputStream() != null) fileStreamName.getInputStream().close();
				mfiles.add(fileStreamName.getFileName());
			}
		}
		return mfiles;
	}

	/**
	 * Gets names from file system and sub-archives.
	 * @param fileName
	 * @param outherPathNeedsInputStream
	 * @return
	 * @throws IOException
	 */
    private List<FileStreamName> innerFileNames(String fileName, boolean outherPathNeedsInputStream) throws IOException {
    	// result list for non-archive files
        List<FileStreamName> fileStreamNames = new ArrayList<FileStreamName>();
        
		// get inner source
		String originalFileName = fileName;
		Matcher matcher = FileURLParser.getURLMatcher(fileName);
		String innerSource;
		int iPreName = 0;
		int iPostName = 0;
		if (matcher != null && (innerSource = matcher.group(5)) != null) {
			iPreName = (matcher.group(2) + matcher.group(3)).length()+1;
			iPostName = iPreName + innerSource.length();
			fileStreamNames = innerFileNames(innerSource, outherPathNeedsInputStream 
					|| fileName.contains("" + WCARD_CHAR[0]) || fileName.contains("" + WCARD_CHAR[1]));
		} else {
			// for archives without ...:(......), just ...:......
			Matcher archMatcher = getArchiveURLMatcher(fileName);
			if (archMatcher != null && (innerSource = archMatcher.group(3)) != null) {
				iPreName = archMatcher.group(2).length()+1;
				iPostName = iPreName + innerSource.length();
				fileStreamNames = innerFileNames(innerSource, outherPathNeedsInputStream 
						|| fileName.contains("" + WCARD_CHAR[0]) || fileName.contains("" + WCARD_CHAR[1]));
			} else if (archMatcher != null && (innerSource = archMatcher.group(7)) != null) {
				iPreName = archMatcher.group(6).length()+1;
				iPostName = iPreName + innerSource.length();
				fileStreamNames = innerFileNames(innerSource, outherPathNeedsInputStream 
						|| fileName.contains("" + WCARD_CHAR[0]) || fileName.contains("" + WCARD_CHAR[1]));
			}
		}
		
		// get archive type
		StringBuilder sbAnchor = new StringBuilder();
		StringBuilder sbInnerInput = new StringBuilder();
		ArchiveType archiveType = FileUtils.getArchiveType(fileName, sbInnerInput, sbAnchor);
		String anchor = sbAnchor.toString();
		if (archiveType != null && iPreName == 0) {
			iPreName = archiveType.name().length()+1;
			iPostName = fileName.length() - (anchor.length() == 0 ? 0 : anchor.length()+1);
		}
		fileName = sbInnerInput.toString();

        //open channels for all filenames
		List<String> newFileNames = resolveAndSetFileNames(fileName); // returns simple file names
        if (fileStreamNames.size() == 0) {
        	for (String newFileName: newFileNames) {
                URL url = FileUtils.getFileURL(parent, newFileName);
            	if ((outherPathNeedsInputStream || (anchor.contains("" + WCARD_CHAR[0]) || anchor.contains("" + WCARD_CHAR[1]))))
            		fileStreamNames.add(new FileStreamName(newFileName, FileUtils.getAuthorizedConnection(url).getInputStream()));
            	else fileStreamNames.add(new FileStreamName(newFileName));
        	}
        }
        
        // check sub-archives
        List<FileStreamName> newFileStreamNames = new ArrayList<FileStreamName>();
        for (FileStreamName fileStreamName: fileStreamNames) {
            // zip archive
            if (archiveType == ArchiveType.ZIP) {
            	processZipArchive(fileStreamName, originalFileName, anchor, iPreName, iPostName, newFileStreamNames);
            
            // gzip archive
            } else if (archiveType == ArchiveType.GZIP) {
            	processGZipArchive(fileStreamName, originalFileName, iPreName, iPostName, newFileStreamNames);
            	
            // tar archive
            } else if (archiveType == ArchiveType.TAR) {
            	processTarArchive(fileStreamName, originalFileName, anchor, iPreName, iPostName, newFileStreamNames);
            
            // return original names
            } else {
            	processProxy(fileStreamName, originalFileName, fileStreamNames);
            	
               	return fileStreamNames;
            }
        }
        return newFileStreamNames;
    }

    /**
     * Verifies and gets proxy file.
     * @param fileStreamName
     * @param originalFileName
     * @param fileStreamNames 
     */
    private void processProxy(FileStreamName fileStreamName, String originalFileName, List<FileStreamName> fileStreamNames) {
    	try {
    		new URL(null, fileStreamName.getFileName(), new ProxyHandler());
    	} catch(MalformedURLException e) {
    		return;
    	}
    	fileStreamNames.clear();
    	fileStreamNames.add(new FileStreamName(originalFileName));
	}

	/**
     * Gets list of zip files with full anchor names.
     * @param fileStreamName
     * @param originalFileName
     * @param anchor
     * @param iPreName
     * @param iPostName
     * @param newFileStreamNames
     * @throws IOException
     */
    private void processZipArchive(FileStreamName fileStreamName, String originalFileName, String anchor, int iPreName, int iPostName, List<FileStreamName> newFileStreamNames) throws IOException {
		// add original name
    	if (fileStreamName.getInputStream() == null) {
    		newFileStreamNames.add(new FileStreamName(originalFileName));
    		return;
    	}
    	
		// look into an archive and check the anchor
		List<String> mfiles = new ArrayList<String>();
    	List<InputStream> lis = FileUtils.getZipInputStreams(fileStreamName.getInputStream(), anchor, mfiles);
    	
    	// create list of new names generated from the anchor
    	for (int i=0; lis != null && i<lis.size(); i++) {
    		newFileStreamNames.add(
    				new FileStreamName(
    					(originalFileName.substring(0, iPreName) + fileStreamName.getFileName() +
    						originalFileName.substring(iPostName)).replace(anchor, mfiles.get(i)), 
    					lis.get(i)));
    	}
    }
    
    /**
     * Gets list of tar files with full anchor names.
     * @param fileStreamName
     * @param originalFileName
     * @param anchor
     * @param iPreName
     * @param iPostName
     * @param newFileStreamNames
     * @throws IOException
     */
    private void processTarArchive(FileStreamName fileStreamName, String originalFileName, String anchor, int iPreName, int iPostName, List<FileStreamName> newFileStreamNames) throws IOException {
		// add original name
    	if (fileStreamName.getInputStream() == null) {
    		newFileStreamNames.add(new FileStreamName(originalFileName));
    		return;
    	}
    	
		// look into an archive and check the anchor
		List<String> mfiles = new ArrayList<String>();
    	List<InputStream> lis = FileUtils.getTarInputStreams(fileStreamName.getInputStream(), anchor, mfiles);
    	
    	// create list of new names generated from the anchor
    	for (int i=0; lis != null && i<lis.size(); i++) {
    		newFileStreamNames.add(
    				new FileStreamName(
    					(originalFileName.substring(0, iPreName) + fileStreamName.getFileName() +
    						originalFileName.substring(iPostName)).replace(anchor, mfiles.get(i)), 
    					lis.get(i)));
    	}
    }

    /**
     * Gets gzip file.
     * @param fileStreamName
     * @param originalFileName
     * @param iPreName
     * @param iPostName
     * @param newFileStreamNames
     * @throws IOException
     */
    private void processGZipArchive(FileStreamName fileStreamName, String originalFileName, int iPreName, int iPostName, List<FileStreamName> newFileStreamNames) throws IOException {
		// add original name
    	if (fileStreamName.getInputStream() == null) {
    		newFileStreamNames.add(new FileStreamName(originalFileName));
    		return;
    	}
    	
		// create input stream
    	InputStream is = new GZIPInputStream(fileStreamName.getInputStream(), Defaults.DEFAULT_IOSTREAM_CHANNEL_BUFFER_SIZE);
    	
    	// create list of new names generated from the anchor
    	newFileStreamNames.add(new FileStreamName(
    			originalFileName.substring(0, iPreName) + fileStreamName.getFileName() + originalFileName.substring(iPostName), 
				is));
    }
    
    
    /**
     * Gets list of file names or an original name from file system. 
     * @param fileName
     * @return
     */
	private List<String> resolveAndSetFileNames(String fileName) {
		// check if the filename is a file or something else
		URL url = null;
		try {
			url = FileUtils.getFileURL(parent, fileName);
		} catch (MalformedURLException e) {
			// NOTHING
		}
		
		// wildcards for file protocol
		if (url.getProtocol().equals(FILE)) {
			return getFileNames(fileName);
		}
		
		// wildcards for sftp protocol
		else if (resolveAllNames && url.getProtocol().equals(SFTP)) {
			return getSftpNames(url);
		}
		
		// wildcards for ftp protocol
		else if (resolveAllNames && url.getProtocol().equals(FTP)) {
			try {
				return getFtpNames(new URL(parent, fileName, FileUtils.ftpStreamHandler));	// the FTPStreamHandler is not properly tested but supports 'ls'
			} catch (MalformedURLException e) {
				//NOTHING
			}
		}

		// return original filename
		List<String> mfiles = new ArrayList<String>();
		mfiles.add(fileName);
		return mfiles;
	}

	/**
	 * Gets files from fileName that can contain wildcards or returns original name.
	 * @param url
	 * @return
	 */
	private List<String> getFtpNames(URL url) {
		// result list
		List<String> mfiles = new ArrayList<String>();

		// if no wildcard found, return original name
		if (!hasWildcard(url)) {
			mfiles.add(url.toString());
			return mfiles;
		}

		// get files
		FTPConnection ftpConnection = null;
		try {
			// list files
			ftpConnection = (FTPConnection)url.openConnection();
			FTPFile[] ftpFiles = ftpConnection.ls(url.getFile());				// note: too long operation
			for (FTPFile lsFile: ftpFiles) {
				if (lsFile.getType() == FTPFile.DIRECTORY_TYPE) continue;
				String resolverdFileNameWithoutPath = lsFile.getName();
				
				// replace file name
				String urlPath = url.getFile();

				// find last / or \ or set index to the start position
				// create new filepath
				int lastIndex = urlPath.lastIndexOf('/')+1;
				if (lastIndex <= 0) lastIndex = urlPath.lastIndexOf('\\')+1;
				String newUrlPath = urlPath.substring(0, lastIndex) + resolverdFileNameWithoutPath;
				
				// add new resolved url string
				mfiles.add(url.toString().replace(urlPath, newUrlPath));
			}
		} catch (Throwable e) {
			// return original name
			mfiles.add(url.toString());
		} finally {
			if (ftpConnection != null)
				try {
					ftpConnection.disconnect();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}

		return mfiles;
	}

	/**
	 * Returns true if the URL, in the concrete his file, has a wildcard. 
	 * @param url
	 * @return
	 */
	private boolean hasWildcard(URL url) {
		// check if the url has wildcards
		String fileName = new File(url.getFile()).getName();
		for (int wcardIdx = 0; wcardIdx < WCARD_CHAR.length; wcardIdx++) {
			if (fileName.indexOf("" + WCARD_CHAR[wcardIdx]) >= 0) { // wildcard found
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Gets files from fileName that can contain wildcards or returns original name.
	 * @param url
	 * @return
	 */
	private List<String> getSftpNames(URL url) {
		// result list
		List<String> mfiles = new ArrayList<String>();
		
		// if no wildcard found, return original name
		if (!hasWildcard(url)) {
			mfiles.add(url.toString());
			return mfiles;
		}

		// get files
		SFTPConnection sftpConnection = null;
		try {
			// list files
			sftpConnection = (SFTPConnection)url.openConnection();
			Vector<?> v = sftpConnection.ls(url.getFile());				// note: too long operation
			for (Object lsItem: v) {
				LsEntry lsEntry = (LsEntry) lsItem;
				String resolverdFileNameWithoutPath = lsEntry.getFilename();
				
				// replace file name
				String urlPath = url.getFile();

				// find last / or \ or set index to the start position
				// create new filepath
				int lastIndex = urlPath.lastIndexOf('/')+1;
				if (lastIndex <= 0) lastIndex = urlPath.lastIndexOf('\\')+1;
				String newUrlPath = urlPath.substring(0, lastIndex) + resolverdFileNameWithoutPath;
				
				// add new resolved url string
				mfiles.add(url.toString().replace(urlPath, newUrlPath));
			}
		} catch (Throwable e) {
			// return original name
			mfiles.add(url.toString());
		} finally {
			if (sftpConnection != null) sftpConnection.disconnect();
		}
		
		return mfiles;
	}

	/**
	 * Gets files from fileName that can contain wildcards or returns original name.
	 * @param fileName
	 * @return
	 */
	private List<String> getFileNames(String fileName) {
		// result list
		List<String> mfiles = new ArrayList<String>();

		// directory name and file name part
		StringBuffer dirName = new StringBuffer();
		StringBuffer filePat = new StringBuffer();
		
		// if no wildcards, return original filename
		if (!splitFilePattern(fileName, dirName, filePat)) {	// no wildcards
			mfiles.add(fileName);
			return mfiles;
		}

		// check a directory
		File dir;
		try {
			dir = new File(FileUtils.getFileURL(parent, dirName.toString()).getPath());
		} catch (Exception e) {
			dir = new File(dirName.toString());
		} 
		
		// list the directory and return its files
		if (dir.exists()) {
			FilenameFilter filter = new WcardFilter(filePat.toString());
			String[] curMatch = dir.list(filter);
			Arrays.sort(curMatch);
			for (int fnIdx = 0; fnIdx < curMatch.length; fnIdx++) {
				File f = new File(dir, curMatch[fnIdx]);
				mfiles.add(f.getAbsolutePath());
			}
		}

		return mfiles;
	}
	
	/**
	 * Checks if the name accepts the pattern
	 * @param pattern
	 * @param name
	 * @return
	 */
	public static boolean checkName(String pattern, String name){
		return new WcardFilter(pattern).accept(name);
	}

	/**
	 * Gets context URL.
	 * @return
	 */
	public URL getParent() {
		return parent;
	}

	/**
	 * Sets context URL.
	 */
	public void setParent(URL parent) {
		this.parent = parent;
	}
	
	/**
	 * Filename filter for wildcard matching.
	 */
	private static class WcardFilter implements FilenameFilter {
		
		// Regex pattern equivalent to specified wildcard pattern.
		private Pattern rePattern;
		
		/**
		 * Constructor. Creates regex pattern so that it is equivalent to given wildcard pattern. 
		 * @param str Wildcard pattern. 
		 */
		public WcardFilter(String str) {

			StringBuffer regex = new StringBuffer(str);
			regex.insert(0, REGEX_START_ANCHOR + REGEX_START_QUOTE);
			for (int wcardIdx = 0; wcardIdx < WCARD_CHAR.length; wcardIdx++) {
				regex.replace(0, regex.length(), 
						regex.toString().replace("" + WCARD_CHAR[wcardIdx],
								REGEX_END_QUOTE + REGEX_SUBST[wcardIdx] + REGEX_START_QUOTE));
			}
			regex.append(REGEX_END_QUOTE + REGEX_END_ANCHOR);

			// Create compiled regex pattern
			rePattern = Pattern.compile(regex.toString());
		}

		/**
		 * Part of FilenameFilter interface.
		 */
		public boolean accept(File dir, String name) {
			return rePattern.matcher(name).matches();
		}
		
		public boolean accept(String name){
			return rePattern.matcher(name).matches();
		}
	}

	/**
	 * Supports filename and their input stream.
	 * @author Jan Ausperger (jan.ausperger@javlin.eu)
	 *         (c) OpenSys (www.opensys.eu)
	 */
	private static class FileStreamName {
		
		// name of file
		private String fileName;
		// input stream - optional
		private InputStream inputStream;

		/**
		 * Constructor.
		 * @param fileName
		 */
		public FileStreamName(String fileName) {
			this(fileName, null);
		}

		/**
		 * Constructor.
		 * @param fileName
		 * @param inputStream
		 */
		public FileStreamName(String fileName, InputStream inputStream) {
			this.fileName = fileName;
			this.inputStream = inputStream;
		}
		
		/**
		 * Gets file name.
		 * @return
		 */
		public String getFileName() {
			return fileName;
		}
		
		/**
		 * Gets input stream.
		 * @return
		 */
		public InputStream getInputStream() {
			return inputStream;
		}
	}

	/**
	 * Finds embedded source.
	 * 
	 * Example: 
	 * 		source:      zip:http://linuxweb/~jausperger/employees.dat.zip#employees0.dat
	 *      result: (g1) zip:
	 *              (g2) http://linuxweb/~jausperger/employees.dat.zip
	 *              (g3) #employees0.dat
	 * 
	 * @param source - input/output source
	 * @return matcher or null
	 */
	public static Matcher getArchiveURLMatcher(String source) {
		Matcher matcher = ARCHIVE_SOURCE.matcher(source);
		return matcher.find() ? matcher : null;
	}

}
