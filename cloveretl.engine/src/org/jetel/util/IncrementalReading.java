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
package org.jetel.util;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.datasection.DataSection;
import org.jetel.util.datasection.DataSectionUtil;
import org.jetel.util.file.FileUtils;

/**
 * Provides support for incremental reading.
 * 
 * @author Jan Ausperger (jan.ausperger@javlin.eu)
 *         (c) Javlin, a.s. (www.javlin.eu)
 */
public class IncrementalReading {
	
    private static Log logger = LogFactory.getLog(IncrementalReading.class);

    // main attributes for incremental reading
    private String incrementalFile;
    private String incrementalKey;

    // a context url for the incremental file
	private URL contextURL;

	// inner variables
    private static Map<String, IncrementalData> incrementalProperties;
    private Map<String, String> incrementalValues;

    // data section such as CDATA for input file
    private static DataSection inFileDataSection = new DataSection("InFile");
    
    private FakedInput fakedInput; // because of backward compatibility
    
    /**
     * Constructor.
     * @param incrementalFile
     * @param incrementalKey
     */
    public IncrementalReading(String incrementalFile, String incrementalKey) {
    	this.incrementalFile = incrementalFile;
    	this.incrementalKey = incrementalKey;
	}

    /**
     * Sets context url for the incremental file.
     * @param contextURL
     */
    public void setContextURL(URL contextURL) {
    	this.contextURL = contextURL;
    }
    
    /**
     * Updates incremental value from the previous source.
     * @param sourceName
     * @param position
     */
    public void nextSource(String sourceName, Object position) {
		// update incremental value from previous source
    	if (incrementalValues == null) return;
       	incrementalValues.put(sourceName, position != null ? position.toString() : null);
    }
    
	/**
     * Initializes incremental reading.
     * @throws ComponentNotReadyException
     */
    public void init() throws ComponentNotReadyException {
    	if (incrementalFile == null && incrementalKey != null) throw new ComponentNotReadyException("Incremental file is not defined for the '" + incrementalKey + "' incremental key attribute!");
    	if (incrementalFile != null && incrementalKey == null) throw new ComponentNotReadyException("Incremental key is not defined for the '" + incrementalFile + "' incremental file attribute!");
    	
    	if (incrementalFile == null) return;
    	if (incrementalProperties == null) {
    		incrementalProperties = new HashMap<String, IncrementalData>();
    	}
    	incrementalValues = new HashMap<String, String>();
    	IncrementalData incremental;
    	Properties prop = new Properties();
    	try {
    		prop.load(Channels.newInputStream(FileUtils.getReadableChannel(contextURL, incrementalFile)));
		} catch (IOException e) {
			logger.warn("The incremental file not found or it is corrupted!", e);
		}
		incremental = new IncrementalData(prop);
		incremental.add(incrementalKey);
		incrementalProperties.put(incrementalFile, incremental);
    	
		String incrementalValue = (String) prop.get(incrementalKey);
		if (incrementalValue == null) {
			logger.warn("The incremental key '" + incrementalKey + "' not found!");
			return;
		}

		// if no inFile section found, add empty file name - because of backward compatibility
		if (!DataSectionUtil.containsDataSections(incrementalValue, inFileDataSection)) {
			String[] positions = incrementalValue != null && !incrementalValue.equals("") ? incrementalValue.split(";") : new String[0];
			if (positions.length > 0) fakedInput = new FakedInput();
			for(String position: positions) {
				fakedInput.addPosition(position);
			}

		// parse files and positions
		} else {
			String dataSection;
			int endIndex = 0;
			while ((dataSection = DataSectionUtil.getDataSectionBlock(incrementalValue, inFileDataSection, endIndex)) != null) {
				int startIndex = dataSection.length() + incrementalValue.indexOf(dataSection, endIndex)+1;
				endIndex = incrementalValue.indexOf(';', startIndex);
				endIndex = endIndex == -1 ? incrementalValue.length() : endIndex;		// if no ';' (it is optional), get string length 
				String position = incrementalValue.substring(startIndex, endIndex);
				incrementalValues.put(DataSectionUtil.decodeString(dataSection, inFileDataSection), position);
			}
		}
		try {
			storeIncrementalReading();
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
    }

    /**
     * Gets incremental value of the source.
     * @param iSource
     * @return
     */
    public Object getSourcePosition(String sourceName) {
		if (fakedInput != null) {
			return fakedInput.getNextPosition();
		}
		if (incrementalValues != null) {
			return incrementalValues.get(sourceName);
		}
		return null;
    }
    
    /**
     * Resets incremental reading. 
     * @throws IOException 
     */
    public void reset() throws IOException {
    	storeIncrementalReading();
    	if (incrementalValues != null)
    		incrementalValues.clear();
		if (fakedInput != null) fakedInput.i = 0;
    }
    
	/**
	 * Updates and stores incremental reading values into a file.
	 * @throws IOException 
	 */
	public void storeIncrementalReading() throws IOException {
		if (incrementalFile == null || incrementalProperties == null || incrementalValues.size() == 0) return;
		
		OutputStream os = Channels.newOutputStream(FileUtils.getWritableChannel(contextURL, incrementalFile, false));
		Properties prop = incrementalProperties.get(incrementalFile).getProperties();
		prop.remove(incrementalKey);
		StringBuilder sb = new StringBuilder();
		for (Entry<String, String> entry: incrementalValues.entrySet()) {
			if (entry.getValue() == null) continue;
			sb.append(DataSectionUtil.encodeString(entry.getKey(), inFileDataSection)).append(':');
			sb.append(entry.getValue()).append(';');
		}
		if (sb.length() > 0) sb.deleteCharAt(sb.length()-1);
		prop.put(incrementalKey, sb.toString());
		prop.store(os, "Incremental reading properties");
		os.flush();
		os.close();
	}
	

    /**
     * The class for incremental reading.
     */
    private static class IncrementalData {
    	// properties for a file
    	private Properties properties;
    	// used keys
    	private Set<String> used;

    	/**
    	 * Constructor
    	 */
    	public IncrementalData(Properties properties) {
    		this.properties = properties;
    		used = new HashSet<String>();
    	}
    	
    	/**
    	 * properties for particular file
    	 */
    	public Properties getProperties() {
    		return properties;
    	}

    	public boolean contains(String key) {
    		return used.contains(key);
    	}
    	public void add(String key) {
    		used.add(key);
    	}
    }

    @Deprecated
    private static class FakedInput {
    	List<String> lPosition = new ArrayList<String>();
    	int i = 0;
    	String getNextPosition() {
    		if (lPosition.size() <= i) return null;
    		return lPosition.get(i++);
    	}
    	void addPosition(String position) {
    		lPosition.add(position);
    	}
    }
}
