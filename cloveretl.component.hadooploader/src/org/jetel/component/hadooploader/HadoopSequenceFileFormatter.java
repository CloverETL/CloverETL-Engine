package org.jetel.component.hadooploader;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.jetel.component.hadooploader.HadoopCloverConvert.Clover2Hadoop;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.hadoop.component.IHadoopSequenceFileFormatter;
import org.jetel.metadata.DataRecordMetadata;

public class HadoopSequenceFileFormatter implements
		IHadoopSequenceFileFormatter {

	private FileSystem dfs;
	private SequenceFile.Writer writer;
	private int keyField;
	private int valueField;
	private String keyFieldName;
	private String valueFieldName;
	private Clover2Hadoop keyCopy;
	private Clover2Hadoop valCopy;
	
	
	public HadoopSequenceFileFormatter(FileSystem dfs) {
		this.dfs=dfs;
	}
	
	public HadoopSequenceFileFormatter(FileSystem dfs,String keyFieldName, String valueFieldName) {
		this.dfs=dfs;
		this.keyFieldName=keyFieldName;
		this.valueFieldName=valueFieldName;
	}
	
	public HadoopSequenceFileFormatter() {
		this.dfs=null;
	}
	
	@Override
	public void init(DataRecordMetadata _metadata)
			throws ComponentNotReadyException {
		if (_metadata.getNumFields()<2){
			throw new ComponentNotReadyException("Hadoop formatter needs metadata with at least 2 fields.");
		}
		if (keyFieldName==null){
			keyField=0;
			valueField=1;
		}else{
			keyField = _metadata.getFieldPosition(keyFieldName);
			valueField = _metadata.getFieldPosition(valueFieldName);
			
			if (keyField == -1){
				throw new ComponentNotReadyException("Can't find key field of name \""+keyFieldName+"\" in metadata.");
			}
			if (valueField == -1){
				throw new ComponentNotReadyException("Can't find value field of name \""+keyFieldName+"\" in metadata.");
			}
		}
		try{
			keyCopy=HadoopCloverConvert.getC2HCopier(_metadata.getField(keyField));
			valCopy=HadoopCloverConvert.getC2HCopier(_metadata.getField(valueField));
		}catch(IOException ex){
			throw new ComponentNotReadyException(ex);
		}

	}

	@Override
	public void reset() {
		// do nothing, we can't reset ?
	}

	@Override
	public void setDataTarget(Object outputDataTarget) throws IOException {
		if (outputDataTarget instanceof SequenceFile.Writer){
			writer=(SequenceFile.Writer)outputDataTarget;
			return;
		}
		if (dfs==null){
			throw new IOException("Can't create output data stream - no Hadoop FileSystem object defined");
		}
		if (outputDataTarget instanceof URI){
			ClassLoader formerContextClassloader = Thread.currentThread().getContextClassLoader();
			try {
				Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
				
				writer = SequenceFile.createWriter(dfs,                    // FileSystem
		                new Configuration(),                  // Configuration
		                new Path((URI)outputDataTarget), // Path to new file in HDFS
		                keyCopy.getValueClass(),     		// Key Data Type
		                valCopy.getValueClass(),            // Value Data Type
		                SequenceFile.CompressionType.NONE);
			} finally {
				Thread.currentThread().setContextClassLoader(formerContextClassloader);
			}
			
		}else if (outputDataTarget instanceof File){
			ClassLoader formerContextClassloader = Thread.currentThread()
					.getContextClassLoader();
			try {
				Thread.currentThread().setContextClassLoader(
						this.getClass().getClassLoader());

				writer = SequenceFile.createWriter(dfs, // FileSystem
						new Configuration(), // Configuration
						new Path(((File) outputDataTarget).getPath()), // Path to  new file in HDFS
						keyCopy.getValueClass(), // Key Data Type
						valCopy.getValueClass(), // Value Data Type
						SequenceFile.CompressionType.NONE);

			} finally {
				Thread.currentThread().setContextClassLoader(
						formerContextClassloader);
			}

		}else{
			throw new IOException("Unsupported data target type: "+outputDataTarget.getClass().getName());
		}

	}

	@Override
	public void close() throws IOException {
		writer.close();
	}

	@Override
	public int write(DataRecord record) throws IOException {
		 keyCopy.setValue(record.getField(keyField));
		 valCopy.setValue(record.getField(valueField));
		 writer.append(keyCopy.getValue(), valCopy.getValue());
		 return 1;
	}

	@Override
	public int writeHeader() throws IOException {
		// no header
		return 0;
	}

	@Override
	public int writeFooter() throws IOException {
		// no footer
		return 0;
	}

	@Override
	public void flush() throws IOException {
		writer.syncFs();
	}

	@Override
	public void finish() throws IOException {
		writer.syncFs();
	}

	@Override
	public boolean isFileTargetPreferred() {
		return true;
	}

	@Override
	public void setAppend(boolean append) {
		// TODO Auto-generated method stub

	}
	
	

	@Override
	public void setKeyValueFields(String keyFieldName, String valueFieldName) {
		this.keyFieldName=keyFieldName;
		this.valueFieldName=valueFieldName;
		
	}
	
}
