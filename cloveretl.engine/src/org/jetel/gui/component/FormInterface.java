/*
 * Created on Apr 4, 2003
 *
 * To change the template for this generated file go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package org.jetel.gui.component;

/**
 * @author administrator
 *
 * To change the template for this generated type comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
public interface  FormInterface {
    
	/**
	 * Typically, data is retrieved from the frame's UI
	 * objects and tested for validity. If there are any
	 * problems, often <code>badDataAlert</code> is used
	 * to communicate it to the user.
	 * <p>
	 * If all of the data is valid, this should return true
	 * so that the caller can proceed (usually by storing
	 * the result somewhere and destroying the frame.)
	 * Naturally, false should be returned if there is
	 * any invalid data.
	 * <p>
	 * @return <code>true</code> if the data in the dialog is acceptable,
	 * <code>false</code> if the data fails to meet validation criteria.
	 *
	 */
	public boolean validateData();
    
	/**
	 * Normally, if the data is valid (see {@link #validateData validateData},)
	 * This is then called to store the data before the dialog is
	 * destroyed.
	 * <p>
	 */
	  public void saveData();


	/**
	 * Used to populate the form with data.
	 * <p>
	 */
	  public void loadData();
}
