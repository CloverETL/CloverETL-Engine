package org.jetel.util;

import java.io.InputStream;
import java.io.PipedInputStream;
import java.util.ArrayList;
import java.util.List;

import org.jetel.data.primitive.ByteArray;

/**
 * Support for the port writing.
 * 
 * @author Jan Ausperger (jan.ausperger@javlin.eu)
 * (c) OpenSys (www.opensys.eu)
 */
public class PortWriting {

	// running threads that reads data from a data formatter
	private List<ReadThread> lReadThread = new ArrayList<ReadThread>();

	// data has to be written to output port completely
	private Object monitor = new Object();

	// data prepared listener
	private DataPreparedListener recordPreparedListener;

	// an exception
	private RuntimeException exception;

	// tmp variables
	private ReadThread tmpReadThread;
	private int lastReturned;

	/**
	 * Data copy from the input port via a data formatter to the byte array.
	 * @param pi
	 */
	public void processData(PipedInputStream pi) {
		if (exception != null) throw exception;
		ReadThread readThread = new ReadThread(pi);
		readThread.setDataPreparedListener(new DataPreparedListener() {
			@Override
			public void dataPrepared(ByteArray bytes) {
				fireDataPrepared();
			}
		});
		readThread.setErrorListener(new ErrorListener() {
			@Override
			public void errorOccured(RuntimeException e) {
				exception = e;
			}
		});
		lReadThread.add(readThread);
		readThread.start();
	}
	
	/**
	 * Waits until all data is sent to output port.
	 */
	public void wait4Finnishing() {
		synchronized (monitor) {
			while (lReadThread.size() != 0 && exception == null) {
				try {
					monitor.wait(100);		//	1ms is good value for output data about kB(s) but for sure if the data is e.g.10GB
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}
		if (exception != null) throw exception;
	}
	
	/**
	 * Fires a listener that sent data to the output port.
	 */
	private synchronized void fireDataPrepared() {
		lastReturned = -1;
		for (int i=0; i<lReadThread.size(); i++) {
			tmpReadThread = lReadThread.get(i);
			if (!tmpReadThread.isFinnished()) break;
			recordPreparedListener.dataPrepared(tmpReadThread.getBytes());
			lastReturned = i;
		}
		for (int i=0; i<=lastReturned; i++) lReadThread.remove(0);
	}
	
	/**
	 * Sets the data prepared listener.
	 * @param recordPreparedListener
	 */
	public void setDataPreparedListener(DataPreparedListener recordPreparedListener) {
		this.recordPreparedListener = recordPreparedListener;
	}

	/**
	 * The main class that reads data from input port and gets byte array from a formatter.
	 * @author Jan Ausperger (jan.ausperger@javlin.eu)
	 * (c) OpenSys (www.opensys.eu)
	 */
	private class ReadThread extends Thread {
		  private InputStream pi = null;
		  private ByteArray bytes;
		  private byte[] buffer;
		  private int len;
		  private DataPreparedListener recordPreparedListener;
		  private ErrorListener errorListener;
		  private boolean finnished;

		  /**
		   * Constructor
		   * @param pi - input stream
		   */
		  private ReadThread(PipedInputStream pi) {
			  buffer = new byte[1024];
			  setName("ReadThread");
			  bytes = new ByteArray();
			  this.pi = pi;
		  }
		  
		  /**
		   * Gets output bytes.
		   * @return
		   */
		  public ByteArray getBytes() {
			  return bytes;
		  }

		  /**
		   * Return true if the reading happened successfully.
		   * @return
		   */
		  private boolean isFinnished() {
			  return finnished;
		  }
		  
		  /**
		   * Reads and writes data.
		   */
		  public synchronized void run() {
			  try {
				  while ((len = pi.read(buffer)) != -1) {
					  bytes.append(buffer, 0, len);
				  }
				  finnished = true;
				  recordPreparedListener.dataPrepared(bytes);
			  } catch (Exception e) {
				  errorListener.errorOccured(e instanceof RuntimeException ? (RuntimeException)e : new RuntimeException(e));
			  }
		  }

		  /**
		   * Sets listeners
		   * @param recordPreparedListener
		   */
		  private void setDataPreparedListener(DataPreparedListener recordPreparedListener) {
			  this.recordPreparedListener = recordPreparedListener;
		  }
		  private void setErrorListener(ErrorListener errorListener) {
			  this.errorListener = errorListener;
		  }
	}

	/**
	 * Class for data prepared listener.
	 */
	public static abstract class DataPreparedListener {
		public DataPreparedListener() {
		}
		public abstract void dataPrepared(ByteArray bytes);
	}

	/**
	 * Class for error listener.
	 */
	private static abstract class ErrorListener {
		public ErrorListener() {
		}
		public abstract void errorOccured(RuntimeException e);
	}
}
