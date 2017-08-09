package org.jetel.component.fileoperation.pool;

public class SMB2ConnectionFactory implements ConnectionFactory {
	
	@Override
	public PoolableConnection makeObject(Authority authority) throws Exception {
		PooledSMB2Connection connection = new PooledSMB2Connection(authority);
		connection.init();
		return connection;
	}

	@Override
	public void destroyObject(Authority authority, PoolableConnection obj) throws Exception {
		PooledSMB2Connection connection = (PooledSMB2Connection) obj;
		connection.disconnect();
	}

	@Override
	public boolean validateObject(Authority authority, PoolableConnection obj) {
		return obj.isOpen();
	}
	@Override
	public boolean supports(Authority authority) {
		return authority.getProtocol().equalsIgnoreCase("smb2");
	}

	@Override
	public void activateObject(Authority arg0, PoolableConnection arg1) throws Exception {
	}

	@Override
	public void passivateObject(Authority arg0, PoolableConnection arg1) throws Exception {
	}

}
