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
package org.jetel.component.transform;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayReader;
import java.io.CharArrayWriter;
import java.io.InputStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.data.ByteDataField;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;
import org.xml.sax.InputSource;

public class XSLTMappingTransition {

	public static final String TRANSFORM = "transform";

	/**
	 * Input parameters.
	 */
	private DataRecord outRecord;
	private String mapping;
	private InputStream xslt;
	private DataRecordMetadata inMetadata;
	private String charset;

	/**
	 * Copy objects.
	 */
	private CopyClover[] copyClover;
	private CopyXstl[] copyXslt;
	private XSLTransformer transformer;
	
	/**
	 * Constructor.
	 * 
	 * @param outRecord
	 * @param mapping
	 * @param xslt
	 */
	public XSLTMappingTransition(DataRecord outRecord, String mapping, InputStream xslt) {
		this.outRecord = outRecord;
		this.mapping = mapping;
		this.xslt = xslt;
	}
	
	/**
	 * Sets input metadata.
	 * 
	 * @param inMetadata
	 */
	public void setInMatadata(DataRecordMetadata inMetadata) {
		this.inMetadata = inMetadata;
	}
	
	/**
	 * Sets a charset.
	 * 
	 * @param charset
	 */
	public void setCharset(String charset) {
		this.charset = charset;
	}
	
	/**
	 * Initializes xslt transition.
	 * 
	 * @throws ComponentNotReadyException
	 */
	public void init() throws ComponentNotReadyException {
		transformer = new XSLTransformer();
		transformer.setCharset(charset);
		try {
			transformer.init(xslt);
		} catch (Exception e) {
			throw new ComponentNotReadyException(e);
		}
		copyClover = createCopyClover(inMetadata, outRecord, mapping);
		copyXslt = createCopyXSLT(inMetadata, outRecord, mapping);
	}

	/**
	 * Checks mapping..
	 * 
	 * @throws ComponentNotReadyException
	 */
	public void checkConfig() throws ComponentNotReadyException {
		createCopyClover(inMetadata, outRecord, mapping);
		createCopyXSLT(inMetadata, outRecord, mapping);
	}
	
	/**
	 * Gets a record.
	 * 
	 * @param inRecord
	 * @return
	 */
	public DataRecord getRecord(DataRecord inRecord) {
		if (inRecord == null) return null;
		outRecord.reset();
		copyClover(inRecord);
		transformXslt(inRecord);
		return outRecord.duplicate();
	}

	private void copyClover(DataRecord inRecord) {
		for (CopyClover copy: copyClover) {
			copy.setDataField(inRecord.getField(copy.getSourcePosition()));
		}
	}
	
	private void transformXslt(DataRecord inRecord) {
		for (CopyXstl copy: copyXslt) {
			copy.setDataField(inRecord.getField(copy.getSourcePosition()));
		}
	}
	
	/**
	 * Creates copy objects.
	 * 
	 * @param inMetadata
	 * @param outRecord
	 * @param mapping
	 * @param xslt
	 * @return
	 * @throws ComponentNotReadyException
	 */
	private CopyObject[] createCopy(DataRecordMetadata inMetadata, DataRecord outRecord, String mapping, boolean xslt) throws ComponentNotReadyException {
		CopyObject copyObject;
		DataField dataField = null;
		int fieldPosition = 0;
		List<CopyObject> lCopyObject = new ArrayList<CopyObject>();
		MappingItarator iterator = new MappingItarator(mapping);
		try {
			iterator.init();
		} catch (Exception e) {
			throw new ComponentNotReadyException(e);
		}
		
		for (Mapping mMapping: iterator) {
			if (xslt) {
				if (!mMapping.isTransform()) continue;
				if (!outRecord.hasField(mMapping.getOutputFieldName())) 
					throw new ComponentNotReadyException("Field '" + mMapping.getOutputFieldName() + "' not found for output port 0.");
				dataField = outRecord.getField(mMapping.getOutputFieldName());
			} else {
				if (mMapping.isTransform()) continue;
				if (!outRecord.hasField(mMapping.getOutputFieldName())) 
					throw new ComponentNotReadyException("Field '" + mMapping.getOutputFieldName() + "' not found for output port 0.");
				dataField = outRecord.getField(mMapping.getOutputFieldName());
			}
			if ((fieldPosition = inMetadata.getFieldPosition(mMapping.getInputFieldName())) == -1) {
				throw new ComponentNotReadyException("Field '" + mMapping.getInputFieldName() + "' not found for input port 0.");
			}
			if (xslt) {
				copyObject = new CopyXstl(dataField);
				((CopyXstl)copyObject).setXslt(transformer);
			} else {
				copyObject = new CopyClover(dataField);
			}
			copyObject.setSourcePosition(fieldPosition);
			lCopyObject.add(copyObject);
		}
		int len = lCopyObject.size();
		CopyObject[] copyObjects = xslt ? new CopyXstl[len] : new CopyClover[len];
		lCopyObject.toArray(copyObjects);
		return copyObjects;
	}

	private CopyClover[] createCopyClover(DataRecordMetadata inMetadata, DataRecord outRecord, String mapping) throws ComponentNotReadyException {
		return (CopyClover[]) createCopy(inMetadata, outRecord, mapping, false);
	}
	
	private CopyXstl[] createCopyXSLT(DataRecordMetadata inMetadata, DataRecord outRecord, String mapping) throws ComponentNotReadyException {
		return (CopyXstl[]) createCopy(inMetadata, outRecord, mapping, true);
	}

	/**
	 * Simple copy class.
	 * 
	 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
	 *         (c) Javlin, a.s. (www.javlin.eu)
	 */
	private abstract static class CopyObject {
		protected DataField targetDataField;
		protected int sourcePosition;
		
		private CopyObject(DataField targetDataField) {
			this.targetDataField = targetDataField;
		}

		/**
		 * Gets position of source field.
		 * 
		 * @return - position of source field
		 */
		public int getSourcePosition() {
			return sourcePosition;
		}
		
		/**
		 * Sets position of source field.
		 * 
		 * @param sourcePosition - position of source field
		 */
		public void setSourcePosition(int sourcePosition) {
			this.sourcePosition = sourcePosition;
		}
		
		public abstract void setDataField(DataField sourceDataField);
		
	}
	
	/**
	 * Simple copy class from clover field to clover field.
	 * 
	 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
	 *         (c) Javlin, a.s. (www.javlin.eu)
	 */
	private static class CopyClover extends CopyObject {

		public CopyClover(DataField dataField) {
			super(dataField);
		}
		
		/**
		 * Copies value from data field into target data field.
		 * 
		 * @param object - source data field
		 */
		@Override
		public void setDataField(DataField sourceDataField) {
			targetDataField.setValue(sourceDataField);
		}
	}
	
	/**
	 * Simple copy class from clover field to clover field.
	 * 
	 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
	 *         (c) Javlin, a.s. (www.javlin.eu)
	 */
	private static class CopyXstl extends CopyObject {

		private boolean isTargetString;
		private boolean isSourceString;
		private boolean isFirst;

		private ByteArrayOutputStream baos;
		private Writer writer;
		private InputStream iStream;
		private InputSource iSource;
		
		private XSLTransformer transformer;
		
		public CopyXstl(DataField dataField) {
			super(dataField);
			isTargetString = !(dataField instanceof ByteDataField);
			isFirst = true;
		}
		
		public void setXslt(XSLTransformer transformer) {
			this.transformer = transformer;
		}
		
		/**
		 * Copies value from data field into target data field.
		 * 
		 * @param object - source data field
		 */
		@Override
		public void setDataField(DataField sourceDataField) {
			Object inValue = sourceDataField.getValue();
			if (isFirst) isSourceString = !(sourceDataField instanceof ByteDataField);
			if (inValue == null) return;
			
			try {
				if (isSourceString) {
					iSource = new InputSource(new CharArrayReader(inValue.toString().toCharArray()));
					if (isTargetString)	transformer.transform(iSource, writer = new CharArrayWriter());
					else transformer.transform(iSource, baos = new ByteArrayOutputStream());
				} else {
					iStream = new ByteArrayInputStream((byte[]) inValue);
					if (isTargetString)	transformer.transform(iStream, writer = new CharArrayWriter());
					else transformer.transform(iStream, baos = new ByteArrayOutputStream());
					
				}
				targetDataField.setValue(isTargetString ? new String(((CharArrayWriter)writer).toCharArray()) : baos.toByteArray());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Mapping iterator.
	 * 
	 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
	 *         (c) Javlin, a.s. (www.javlin.eu)
	 */
	public static class MappingItarator implements Iterable<Mapping> {

		private static final String SEMICOLON_DELIMITER = ";";
		private static final String EQUALS_DELIMITER = ":=";
		private static final String DOT_DELIMITER = "\\.";

		private String mapping;
		List<Mapping> lMapping;
		
		public MappingItarator(String mapping) {
			this.mapping = mapping;
		}
		
		public void init() throws Exception {
			String[] statements = mapping.split(SEMICOLON_DELIMITER);
			String[] fields;
			lMapping = new ArrayList<Mapping>();
			if (mapping.equals("")) return;
			
			Pattern INNER_SOURCE = Pattern.compile(TRANSFORM + "\\((.*)\\)$");
			Mapping mMapping;

			for (String statement: statements) {
				fields = statement.split(EQUALS_DELIMITER);
				if (fields.length != 2) throw new Exception("Wrong mapping statement: '" + statement + "'.");
				Matcher matcher = INNER_SOURCE.matcher(fields[1]);
				boolean isXSLT;
				if (isXSLT = matcher.find()) {
					fields[1] = matcher.group(1);
				} 
				mMapping = new Mapping(getFieldName(fields[1]), getFieldName(fields[0]), isXSLT);
				lMapping.add(mMapping);
			}
		}
		
		@Override
		public Iterator<org.jetel.component.transform.XSLTMappingTransition.Mapping> iterator() {
			return lMapping.iterator();
		}
		
		private String getFieldName(String statement) {
			String[] field = statement.split(DOT_DELIMITER);
			return field.length == 2 ? field[1] : field[0];
		}

	}

	/**
	 * Mapping class for the xslt mapping.
	 * 
	 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
	 *         (c) Javlin, a.s. (www.javlin.eu)
	 */
	public static class Mapping {
		
		private boolean isTransform;
		private String inputFieldName;
		private String outputFieldName;
		
		public Mapping(String inputFieldName, String outputFieldName, boolean isTransform) {
			this.inputFieldName = inputFieldName;
			this.outputFieldName = outputFieldName;
			this.isTransform = isTransform;
		}

		public boolean isTransform() {
			return isTransform;
		}
		
		public String getInputFieldName() {
			return inputFieldName;
		}

		public String getOutputFieldName() {
			return outputFieldName;
		}

	}

	
}
