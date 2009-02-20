package org.jetel.data.parser;

import java.nio.BufferUnderflowException;
import java.nio.CharBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Default MultiLevelSelector implementation
 * 
 * @author pnajvar
 *
 */
public class PrefixMultiLevelSelector implements MultiLevelSelector {

	Properties prefix2meta;
	DataRecordMetadata[] metadataPool;
	int minKeyLength;
	int maxKeyLength;
	int nextOffset;
	TreeMap<Integer, HashMap<String, Integer>> keys = new TreeMap<Integer, HashMap<String, Integer>>();
	int nextRecordMetadataIndex = -1;
	
	public void choose(CharBuffer data) throws BufferUnderflowException {
		
		// read at least minKeyLength
		char[] chars = new char[maxKeyLength];
		int rkl = 0;
		// if buffer underruns here... it is a problem
		data.get(chars, rkl, minKeyLength);
		rkl = minKeyLength;
		
		// now go through keys and try to match
		// must go through all of them because we allow prefix keys
		// i.e. keys "aa" and "aab" are allowed at the same time
		// so we take the longest possible match
		String inputString;
		for (Map.Entry<Integer, HashMap<String, Integer>> e : keys.entrySet()) {

			if (rkl < e.getKey().intValue()) {
				// we must read more characters
				data.get(chars, rkl, e.getKey().intValue() - rkl);
				rkl = e.getKey().intValue();
			}

			inputString = new String(chars, 0, rkl);

			for (Map.Entry<String, Integer> e2 : e.getValue().entrySet()) {
				// lets try all the keys of length rkl
				if (e2.getKey().equals(inputString)) {
					nextRecordMetadataIndex = e2.getValue().intValue();
				}
			}

		}
		
	}

	public void init(DataRecordMetadata[] metadata, Properties properties) throws ComponentNotReadyException {
		this.reset();
		this.metadataPool = metadata;
		this.prefix2meta = parseProperties(properties);
	}

	/**
	 * Verifies whether "prop" is correct set of properties
	 * 
	 * It must be a structure of prefixes as keys and output port numbers as values
	 * Each key should not be a prefix of any other key
	 * 
	 * @param prop
	 * @return
	 * @throws ComponentNotReadyException
	 */
	Properties parseProperties(Properties prop) throws ComponentNotReadyException {

		if (prop.size() == 0) {
			throw new ComponentNotReadyException("Prefix mapping table must not be empty");
		}
		
		String skey;
		String svalue;
		Integer tmpi;
		minKeyLength = Integer.MAX_VALUE;
		maxKeyLength = Integer.MIN_VALUE;
		int tmpkl;
		for(Map.Entry<Object, Object> e: prop.entrySet()) {
			skey = e.getKey().toString();
			tmpkl = skey.length();
			
			if (tmpkl < minKeyLength) {
				minKeyLength = tmpkl;
			}
			if (tmpkl > maxKeyLength) {
				maxKeyLength = tmpkl;
			}
			svalue = e.getValue().toString();
			try {
				tmpi = Integer.valueOf(svalue);
			} catch (NumberFormatException e1) {
				throw new ComponentNotReadyException("Property value of '" + skey + "' must be a number (not '" + svalue + "')");
			}
			
			// remember the key by its length
			if (! keys.containsKey(tmpkl)) {
				HashMap<String, Integer> tmpmap = new HashMap<String, Integer>();
				tmpmap.put(skey, tmpi);
				keys.put(tmpkl, tmpmap);
			} else {
				keys.get(tmpkl).put(skey, tmpi);
			}

		}
		return prop;
	}
	
	public int lookAheadCharacters() {
		// will request maxKeyLength so that we ensure
		// most will be able to tell without buffer underruns
		return maxKeyLength;
	}

	public int nextRecordOffset() {
		return nextOffset;
	}

	public void reset() {
		this.nextOffset = 0;
		this.nextRecordMetadataIndex = -1;
	}

	public int nextRecordMetadataIndex() {
		return nextRecordMetadataIndex;
	}

}
