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
package org.jetel.metadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;

import org.jetel.exception.JetelRuntimeException;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.stream.StreamUtils;
import org.jetel.util.string.StringUtils;

/**
 * Various utilities for data record metadata manipulation.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21. 1. 2014
 */
public class MetadataUtils {

    /**
     * Reads data record metadata from given channel. It is expected that the channel contains first an integer
     * which indicates length of following XML metadata definition. Arbitrary data can follow the metadata definition in the stream.
     * Note: if the stream contains only the XML metadata definition without the header with length of bytes,
     * use directly DataRecordMetadataXMLReaderWriter.readMetadata() instead.
     */
    public static DataRecordMetadata readMetadata(ReadableByteChannel input) throws IOException {
    	ByteBuffer metadataSizeBuffer = ByteBuffer.allocate(4);
    	if (StreamUtils.readBlocking(input, metadataSizeBuffer) != 4) {
    		throw new JetelRuntimeException("Metadata information is not available in data stream.");
    	}
    	metadataSizeBuffer.flip();
    	int metadataSize = metadataSizeBuffer.getInt();
    	
    	ByteBuffer metadataBuffer = ByteBuffer.allocate(metadataSize);
    	
    	if (StreamUtils.readBlocking(input, metadataBuffer) != metadataSize) {
    		throw new JetelRuntimeException("Metadata information is not available in data stream.");
    	}

    	metadataBuffer.flip();
    	byte[] metadataBytes = new byte[metadataSize];
    	metadataBuffer.get(metadataBytes);
    	
    	return DataRecordMetadataXMLReaderWriter.readMetadata(new ByteArrayInputStream(metadataBytes));
    }
    
    /**
     * Creates data record metadata based on a string with XML definition.
     * @param input XML definition of metadata
     * @return data record metadata
     * @throws IOException
     */
    public static DataRecordMetadata readMetadata(String input) {
    	try {
			return DataRecordMetadataXMLReaderWriter.readMetadata(new ByteArrayInputStream(input.getBytes(DataRecordMetadataXMLReaderWriter.DEFAULT_CHARACTER_ENCODING)));
		} catch (UnsupportedEncodingException e) {
			throw new JetelRuntimeException(e);
		}
    }
    
    /**
     * Writes the given metadata to the given output channel. First, length of XML representation of metadata is written
     * and after that the XML representation of metadata is written.
     * @param metadata written metadata
     * @param outputChannel target channel
     * @throws IOException
     */
    public static void writeMetadata(DataRecordMetadata metadata, WritableByteChannel outputChannel) throws IOException {
    	//serialise metadata to xml
    	ByteArrayOutputStream metadataStream = new ByteArrayOutputStream();
    	DataRecordMetadataXMLReaderWriter.write(metadata, metadataStream);
    	byte[] metadataBytes = metadataStream.toByteArray();

    	CloverBuffer result = CloverBuffer.allocate(metadataBytes.length + 4);

    	//write length of xml of metadata
    	result.putInt(metadataBytes.length);
    	//write xml of metadata
    	result.put(metadataBytes);

    	result.flip();
    	outputChannel.write(result.buf());
    }
    
    /**
     * Returns a duplicate of the given metadata
     * with auto-filled fields removed.
     * 
     * @param metadata
     * @return metadata containing only fields without auto-filling
     */
    public static DataRecordMetadata getNonAutofilledFieldsMetadata(DataRecordMetadata metadata) {
    	return getSelectedFieldsMetadata(metadata, metadata.getNonAutofilledFields());
    }
    
    /**
     * Returns a duplicate of the given metadata
     * containing only fields with the selected indexes.
     * 
     * @param metadata
     * @return metadata containing only selected fields
     */
    public static DataRecordMetadata getSelectedFieldsMetadata(DataRecordMetadata metadata, int[] selectedFields) {
    	DataRecordMetadata result = metadata.duplicate();
    	result.delAllFields();
    	for (int i: selectedFields) {
    		result.addField(metadata.getField(i).duplicate());
    	}
    	return result;
    }

    /**
     * This comparison is very slow - conversion to XML is used for metadata comparison.
     * @param metadata1
     * @param metadata2
     * @return true if and only if the given metadata are identical
     */
    public static boolean equals(DataRecordMetadata metadata1, DataRecordMetadata metadata2) {
		if (metadata1 == null || metadata2 == null) {
			throw new NullPointerException("null metadata");
		}
		String strMetadata1 = serializeMetadata(metadata1);
		String strMetadata2 = serializeMetadata(metadata2);
		return strMetadata1.equals(strMetadata2);
    }
    
    /**
     * Serializes metadata to XML
     * @param metadata
     * @return
     */
    public static String serializeMetadata(DataRecordMetadata metadata) {
		try {
	    	ByteArrayOutputStream os = new ByteArrayOutputStream();
	    	DataRecordMetadataXMLReaderWriter.write(metadata, os);
			return os.toString(DataRecordMetadataXMLReaderWriter.DEFAULT_CHARACTER_ENCODING);
		} catch (UnsupportedEncodingException e) {
			throw new JetelRuntimeException("Metadata " + metadata.getId() + " serialization failed.", e);
		}
    }
    
    /**
     * CLO-9269
	 * Produces concatenation of metadata.
	 * 
	 * Result metadata are made by copying first metadata and copying all fields from subsequent metadata.
	 * 
	 * Delimiters are normalized to produce consistent metadata (all fields with same delimiter)
	 */
    public static DataRecordMetadata getConcatenatedMetadata(DataRecordMetadata[] inputMetadata, String resultMetaName) {
    	if (inputMetadata == null || inputMetadata.length == 0) {
    		throw new JetelRuntimeException("Metadata concatenation failed");
    	}
    	
    	boolean fixLen = true; // should result metadata be fix length?
    	for (DataRecordMetadata meta : inputMetadata) {
    		if (meta.getParsingType() != DataRecordParsingType.FIXEDLEN) {
    			fixLen = false;
    			break;
    		}
    	}
    	
    	DataRecordMetadata outMeta = inputMetadata[0].duplicate();
    	ArrayList<String> outFieldNameList = new ArrayList<>(Arrays.asList(inputMetadata[0].getFieldNamesArray()));
		if (!fixLen) {
			if (outMeta.getParsingType() != DataRecordParsingType.DELIMITED) {
				outMeta.setParsingType(DataRecordParsingType.DELIMITED);
			}
			if (outMeta.getFieldDelimiter() == null) {
				outMeta.setFieldDelimiter("|");
			}
			if (outMeta.getRecordDelimiter() == null) {
				outMeta.setRecordDelimiter(System.getProperty("line.separator"));
			}
		}
		
		// add fields from remaining inputs
		for (int i = 1; i < inputMetadata.length; i++) {
			for (DataFieldMetadata inFieldMeta : inputMetadata[i].getFields()) {
				outMeta.addField(inFieldMeta.duplicate());
				outFieldNameList.add(inFieldMeta.getName());
			}
		}
		
		outMeta.getRecordProperties().clear(); // clear GUI properties (preview attachment etc.)
		
		outMeta.setName(resultMetaName);
		String[] normalizedNames = StringUtils.normalizeNames(outFieldNameList); // normalize field names (handles duplicate field names)
		for (int i = 0; i < normalizedNames.length; i++) {
			DataFieldMetadata field = outMeta.getField(i);
			field.setName(normalizedNames[i]);
			if (!fixLen) {
				field.setSize(0);
			}
			field.setDelimiter(null);
		}
		
		// add and remove a field to force rebuild of name index - this is necessary because we changed names of some fields
		outMeta.addField(new DataFieldMetadata("dummy", null));
		outMeta.delField(outMeta.getNumFields() - 1);
		return outMeta;
	}
    
}
