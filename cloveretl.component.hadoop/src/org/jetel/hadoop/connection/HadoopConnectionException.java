package org.jetel.hadoop.connection;

public class HadoopConnectionException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6547597772079128839L;

	public HadoopConnectionException(Exception e) {
		super(e);
	}
	
	public HadoopConnectionException(String reason, Exception e) {
		super(reason, e);
	}
	
	/**
	 * @param string
	 */
	public HadoopConnectionException(String reason) {
		super(reason);
	}	
	
}
