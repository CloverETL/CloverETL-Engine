package org.jetel.util;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.file.FileUtils;

/**
 * Provides support for incremental reading.
 * 
 * @author Jan Ausperger (jan.ausperger@javlin.eu)
 *         (c) OpenSys (www.opensys.eu)
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
    private String[] incrementalInValues;
    private ArrayList<String> incrementalOutValues;

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
     * @param iSource
     * @param position
     */
    public void nextSource(int iSource, Object position) {
		// update incremental value from previous source
		if (incrementalOutValues != null && iSource >= 0) {
			for (int i=incrementalOutValues.size(); i<iSource; i++) incrementalOutValues.add(null);
			if (iSource < incrementalOutValues.size()) incrementalOutValues.remove(iSource);
			incrementalOutValues.add(iSource, position != null ? position.toString() : null);
		}
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
    	IncrementalData incremental;
    	Properties prop = new Properties();
    	try {
    		prop.load(Channels.newInputStream(FileUtils.getReadableChannel(contextURL, incrementalFile)));
		} catch (IOException e) {
			logger.warn("The incremental file not found or it is corrupted! Cause: " + e.getMessage());
		}
		incremental = new IncrementalData(prop);
		incremental.add(incrementalKey);
		incrementalProperties.put(incrementalFile, incremental);
    	
		String incrementalValue = (String) prop.get(incrementalKey);
		if (incrementalValue == null) {
			logger.warn("The incremental key '" + incrementalKey + "' not found!");
		}
		incrementalInValues = incrementalValue != null && !incrementalValue.equals("") ? incrementalValue.split(";") : new String[0];
		incrementalOutValues = new ArrayList<String>();
		for (String value: incrementalInValues) {
			incrementalOutValues.add(value);
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
    public Object getSourcePosition(int iSource) {
		if (incrementalInValues != null && iSource < incrementalInValues.length) {
			return incrementalInValues[iSource];
		}
		return null;
    }
    
    /**
     * Resets incremental reading. 
     * @throws IOException 
     */
    public void reset() throws IOException {
    	storeIncrementalReading();
		if (incrementalOutValues != null) {
			incrementalInValues = new String[incrementalOutValues.size()];
			incrementalOutValues.toArray(incrementalInValues);
		}
    }
    
	/**
	 * Updates and stores incremental reading values into a file.
	 * @throws IOException 
	 */
	public void storeIncrementalReading() throws IOException {
		if (incrementalFile == null || incrementalProperties == null) return;
		
		OutputStream os = Channels.newOutputStream(FileUtils.getWritableChannel(contextURL, incrementalFile, false));
		Properties prop = incrementalProperties.get(incrementalFile).getProperties();
		prop.remove(incrementalKey);
		StringBuilder sb = new StringBuilder();
		for (String value: incrementalOutValues) sb.append(value).append(";");
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

}
