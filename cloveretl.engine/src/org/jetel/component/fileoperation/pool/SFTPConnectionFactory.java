package org.jetel.component.fileoperation.pool;

public class SFTPConnectionFactory implements ConnectionFactory {

	private static final String SFTP_SCHEME = "sftp";
	
	@Override
	public PoolableConnection makeObject(Authority authority) throws Exception {
		SFTPConnection connection = new SFTPConnection(authority);
		connection.connect();
		return connection;
	}

	@Override
	public void destroyObject(Authority key, PoolableConnection obj)
			throws Exception {
		((SFTPConnection) obj).disconnect();
	}

	@Override
	public boolean validateObject(Authority key, PoolableConnection obj) {
		return ((SFTPConnection) obj).isOpen();
	}

	@Override
	public void activateObject(Authority key, PoolableConnection obj)
			throws Exception {
	}

	@Override
	public void passivateObject(Authority key, PoolableConnection obj)
			throws Exception {
	}

	@Override
	public boolean supports(Authority authority) {
		return authority.getProtocol().equalsIgnoreCase(SFTP_SCHEME);
	}

}
