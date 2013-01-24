package org.jetel.component.fileoperation.pool;


public interface Authority {
	
	public String getProtocol();

	public String getUserInfo();

	public String getHost();

	public int getPort();
	
	public String getProxyString();

	public boolean equals(Object o);
	
	public int hashCode();
	
}
