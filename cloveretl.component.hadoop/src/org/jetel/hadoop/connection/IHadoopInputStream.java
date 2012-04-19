package org.jetel.hadoop.connection;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

public interface IHadoopInputStream extends DataInput, Closeable {

	public DataInputStream getDataInputStream();
	
	public void seek(long position) throws IOException;
	
	public boolean seekToNewSource(long targetPos) throws IOException; 
	
	public long getPos() throws IOException ; 
	
	
	
}
