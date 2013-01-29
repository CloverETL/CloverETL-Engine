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
package org.jetel.component.fileoperation;


public class PooledSFTPOperationHandlerTest extends SFTPOperationHandlerTest {

	private PooledSFTPOperationHandler handler = null;

	@Override
	protected IOperationHandler createOperationHandler() {
		return handler = new PooledSFTPOperationHandler();
	}
	
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		handler = null;
	}

	@Override
	public void testGetPriority() {
//		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getSpeed(Operation.copy(PooledSFTPOperationHandler.SFTP_SCHEME, PooledSFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.move(PooledSFTPOperationHandler.SFTP_SCHEME, PooledSFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.delete(PooledSFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.create(PooledSFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.resolve(PooledSFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.info(PooledSFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.list(PooledSFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.read(PooledSFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.write(PooledSFTPOperationHandler.SFTP_SCHEME)));
	}

	@Override
	public void testCanPerform() {
//		assertTrue(handler.canPerform(Operation.copy(PooledSFTPOperationHandler.SFTP_SCHEME, PooledSFTPOperationHandler.SFTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.move(PooledSFTPOperationHandler.SFTP_SCHEME, PooledSFTPOperationHandler.SFTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.delete(PooledSFTPOperationHandler.SFTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.create(PooledSFTPOperationHandler.SFTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.resolve(PooledSFTPOperationHandler.SFTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.info(PooledSFTPOperationHandler.SFTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.list(PooledSFTPOperationHandler.SFTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.read(PooledSFTPOperationHandler.SFTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.write(PooledSFTPOperationHandler.SFTP_SCHEME)));
//		
//		try {
//			Thread.sleep(60000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}

}
