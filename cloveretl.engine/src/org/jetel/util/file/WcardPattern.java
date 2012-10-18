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
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.commons.net.ftp.FTPFile;
import org.jetel.data.Defaults;
import org.jetel.enums.ArchiveType;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.runtime.IAuthorityProxy;
import org.jetel.util.protocols.amazon.S3InputStream;
import org.jetel.util.protocols.ftp.FTPConnection;
import org.jetel.util.protocols.proxy.ProxyHandler;
import org.jetel.util.protocols.sftp.SFTPConnection;
import org.jetel.util.protocols.webdav.WebdavOutputStream;
import org.jetel.util.string.StringUtils;

import com.googlecode.sardine.DavResource;
import com.googlecode.sardine.Sardine;
import com.googlecode.sardine.SardineFactory;
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
	
	// http protocol
	private final static String HTTP = "http";

	// https protocol
	private final static String HTTPS = "https";

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
	public static void splitFilePattern(String pat, StringBuffer dir, StringBuffer filePat) {
		dir.setLength(0);
		filePat.setLength(0);

		File f = new File(pat);
		String parent = f.getParent();
		dir.append(StringUtils.isEmpty(parent) ? "." : parent); // CLD-4114: When parent == null, dir would be "null"
		filePat.append(f.getName());
	}
	
	/**
	 * Returns index of the first wildcard.
	 * @param filePat
	 * @return
	 */
	private int getWildCardIndex(String filePat) {
		int resIdx = -1;
		for (int wcardIdx = 0; wcardIdx < WCARD_CHAR.length; wcardIdx++) {
			int i;
			if ((i = filePat.indexOf("" + WCARD_CHAR[wcardIdx])) >= 0) { // wildcard found
				resIdx = resIdx == -1 || resIdx > i ? i : resIdx;
			}
		}
		// no wildcard in pattern
		return resIdx;
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
    	InputStream is = new GZIPInputStream(fileStreamName.getInputStream(), Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
    	
    	// create list of new names generated from the anchor
    	newFileStreamNames.add(new FileStreamName(
    			originalFileName.substring(0, iPreName) + fileStreamName.getFileName() + originalFileName.substring(iPostName), 
				is));
    }
    
	private List<String> getSanboxNames(URL url) {
		List<String> fileStreamNames = new ArrayList<String>();
		if (hasWildcard(url)) {
			IAuthorityProxy authorityProxy = IAuthorityProxy.getAuthorityProxy(ContextProvider.getGraph());
			String storageCode = url.getHost();
			String queryPart = url.getQuery() != null ? "?" + url.getQuery() : "";
			for (String fileName : authorityProxy.resolveAllFiles(storageCode, url.getPath() + queryPart)) {
				fileStreamNames.add(SandboxUrlUtils.SANDBOX_PROTOCOL_URL_PREFIX + fileName);
			}
		} else {
			fileStreamNames.add(url.toString());
		}
		return fileStreamNames;
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
		
		else if (resolveAllNames && (
				url.getProtocol().equals(SandboxUrlUtils.SANDBOX_PROTOCOL))) {
			return getSanboxNames(url);
		}
		else if (resolveAllNames && (
				url.getProtocol().equals(HTTP) || url.getProtocol().equals(HTTPS))) {
			return getHttpNames(url);
		}

		// return original filename
		List<String> mfiles = new ArrayList<String>();
		mfiles.add(fileName);
		return mfiles;
	}
	
	// matches the FILENAME part of a path: /dir/subdir/subsubdir/FILENAME
	private static final Pattern FILENAME_PATTERN = Pattern.compile(".*/([^/]+/?)");

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
				String resolverdFileNameWithoutPath = lsFile.getName().replace('\\', '/');
				Matcher m = FILENAME_PATTERN.matcher(resolverdFileNameWithoutPath);
				if (m.matches()) {
					resolverdFileNameWithoutPath = m.group(1); // CL-2468 - some FTPs return full path as file name, get rid of the path
				}
				
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
	
	private boolean hasAsteriskWildcard(URL url) {
		String topmostFile = new File(url.getFile()).getName();
		return topmostFile.indexOf('*') != -1;
	}
	
	/**
	 * Gets files from HTTP source using WebDAV 
	 */
	private List<String> getHttpNames(URL url) {
		List<String> mfiles = new ArrayList<String>();

		// We will only consider asterisk (*) as a wild-card in HTTP URL.
		// Question mark (?) serves as a query separator.
		
		if (S3InputStream.isS3File(url)) {
			if (hasAsteriskWildcard(url)) {
				try {
					return S3InputStream.getObjects(url);
				} catch (IOException e) {
					mfiles.add(url.toString());
					return mfiles;
				}
			}
			else {
				mfiles.add(url.toString());
				return mfiles;
			}
		}
		
		if (!hasAsteriskWildcard(url)) {
			mfiles.add(url.toString());
			return mfiles;
		}
		
		// When there is asterisk wildcard, we will presume the user wants to use WebDAV access to list all the files.
		try {
			Sardine sardine = SardineFactory.begin(WebdavOutputStream.getUsername(url), WebdavOutputStream.getPassword(url));
			sardine.enableCompression();
			String file = url.getFile();
			int lastSlash = file.lastIndexOf('/');
			if (lastSlash == -1) {
				throw new IllegalArgumentException("Unexpected format of URL");
			}
			// The issue with sardine is that user authorization must be passed in begin() of SardineFactory
			// but cannot be kept as part of the URL for which we try to get resources.
			// And finally, later we need the authorization details in the URL 
			// to actually access the file.
			// So, typically, for anonymous access a URL like http://anonymous:@host/ is required
			String dir = file.substring(0, lastSlash + 1);

			// remove authorization info 
			String dirURL = url.getProtocol() + "://" + url.getHost() + ":" + url.getPort() + dir;
			String pattern = url.getFile();
			
			List<DavResource> resources = sardine.getResources(dirURL);
			for (DavResource res : resources) {
				if (res.isDirectory()) {
					continue;
				}
				if (checkName(pattern, res.getPath())) {
					// add authorization info back
					String fullURL = url.getProtocol() + "://" + url.getAuthority() + dir + res.getName();
					mfiles.add(fullURL);
				}
			}
		} catch (IOException e) {
			// it was not possible to connect using WebDAV, let's presume it's a normal HTTP request
			mfiles.add(url.toString());
			return mfiles;
		}
		
		return mfiles;
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
		splitFilePattern(fileName, dirName, filePat);
		int idxFileWildCard = getWildCardIndex(filePat.toString());
		int idxPathWildCard = getWildCardIndex(dirName.toString());
		if (idxFileWildCard < 0 && idxPathWildCard < 0) {	// no wildcards
			mfiles.add(fileName);
			return mfiles;
		}

		// check a directory
		List<File> lResolvedPaths = new LinkedList<File>();
		try {
			URL url = FileUtils.getFileURL(parent, dirName.toString());
			lResolvedPaths.add(new File(url.getQuery() != null ? url.getPath() + "?" + url.getQuery() : url.getPath()));
		} catch (Exception e) {
			lResolvedPaths.add(new File(dirName.toString()));
		} 
		
		// get all valid paths
		if (idxPathWildCard >= 0) {
			String wCardPath = lResolvedPaths.get(0).toString();
			lResolvedPaths.clear();
			lResolvedPaths.addAll(getResolvedPaths(wCardPath));
		}
		
		// list the directory and return its files
		for (File dir: lResolvedPaths) {
			if (dir.exists()) {
				FilenameFilter filter = new WcardFilter(filePat.toString());
				String[] curMatch = dir.list(filter);
				Arrays.sort(curMatch);
				for (int fnIdx = 0; fnIdx < curMatch.length; fnIdx++) {
					File f = new File(dir, curMatch[fnIdx]);
					mfiles.add(f.getAbsolutePath());
				}
			}
		}
		return mfiles;
	}
	
	/**
	 * @param string
	 * @return
	 */
	private Collection<? extends File> getResolvedPaths(String sDir) {
		List<File> lResolvedPaths = new LinkedList<File>();
		int idxPathWildCard = getWildCardIndex(sDir);
		if (idxPathWildCard == -1) {
			lResolvedPaths.add(new File(sDir));
			return lResolvedPaths;
		}
		
		// get resolved path
		String sTmp = sDir.substring(0, idxPathWildCard);
		File fTmp = new File(sTmp);
		File sResolvedPath = sTmp.endsWith("/") || sTmp.endsWith("\\") ? fTmp : fTmp.getParentFile();
		
		// get wildcard dir name
		File tmpFile = new File(sDir);
		File parentFile;
		do {
			parentFile = tmpFile;
			tmpFile = parentFile.getParentFile();
		} while (!sResolvedPath.equals(tmpFile));
		String sWCardPath = parentFile.getName();
		
		// list and add all suitable directories directories
		FilenameFilter filter = new WcardFilter(sWCardPath);
		File[] curMatch = sResolvedPath.listFiles(filter);
		if (curMatch != null) {
			Arrays.sort(curMatch);
			for (File f: curMatch) {
				if (f.isDirectory()) {
					lResolvedPaths.addAll(getResolvedPaths(f.toString() + sDir.substring(parentFile.toString().length())));
				}
			}
		}
		return lResolvedPaths;
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
	 * Creates compiled Pattern from String pattern with simplified syntax -- containing '*' and '?' symbols.
	 * 
	 * @param pattern
	 * @return
	 */
	public static Pattern compileSimplifiedPattern(String pattern) {
		StringBuilder regex = new StringBuilder(pattern);
		regex.insert(0, REGEX_START_ANCHOR + REGEX_START_QUOTE);
		for (int wcardIdx = 0; wcardIdx < WCARD_CHAR.length; wcardIdx++) {
			regex.replace(0, regex.length(), regex.toString().replace("" + WCARD_CHAR[wcardIdx],
					REGEX_END_QUOTE + REGEX_SUBST[wcardIdx] + REGEX_START_QUOTE));
		}
		regex.append(REGEX_END_QUOTE + REGEX_END_ANCHOR);

		return Pattern.compile(regex.toString());
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
			rePattern = compileSimplifiedPattern(str);
		}

		/**
		 * Part of FilenameFilter interface.
		 */
		@Override
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
	 *         (c) Javlin, a.s. (www.javlin.eu)
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
