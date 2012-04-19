package org.jetel.hadoop.connection;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

public interface IHadoopOutputStream extends DataOutput, Closeable {

	public DataOutputStream getDataOutputStream();
	
	public long getPos() throws IOException;
	
}
