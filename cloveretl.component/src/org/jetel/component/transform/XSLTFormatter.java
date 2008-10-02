package org.jetel.component.transform;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import org.jetel.data.DataRecord;
import org.jetel.data.formatter.Formatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

public class XSLTFormatter implements Formatter {
	
	private WritableByteChannel writableByteChannel;
	
	public void close() {
		try {
			writableByteChannel.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void finish() throws IOException {
		close();
	}
	
	public void flush() throws IOException {
	}
	
	public void init(DataRecordMetadata _metadata) throws ComponentNotReadyException {
	}
	
	public void reset() {
	}
	
	public void setDataTarget(Object outputDataTarget) {
		writableByteChannel = (WritableByteChannel)outputDataTarget;
	}
	
	public int write(DataRecord record) throws IOException {
		return 0;
	}
	
	public int writeFooter() throws IOException {
		return 0;
	}
	
	public int writeHeader() throws IOException {
		return 0;
	}

	public WritableByteChannel getWritableByteChannel() {
		return writableByteChannel;
	}
}
