/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2003,2002  David Pavlis, Wes Maciorowski
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
// FILE: c:/projects/jetel/org/jetel/gui/component/PhaseFrame.java
package org.jetel.gui.component;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.border.*;

import org.jetel.gui.fileformatwizard.FileFormatDataModel;
import org.jetel.gui.fileformatwizard.FileFormatDispatcher;

/**
 * Abstraction contains UI necessary to navigate the phased process (Previous,
 * Next, Finish, and Cancel buttons) and supporting structures for subclassers'
 * UI.
 */
public abstract class PhaseFrame extends JFrame {
    
    /**
     * This is the main frame we insert all UI into
     */
    private JPanel backgroundPanel;
    
    /**
     * This panel holds the Next and Previous, Cancel, and other buttons
     */
    protected JPanel navigationPanel;
    
    /**
     * Subclassers should add their components to contentJPanel.
     * Its layout is handled by a Box object with vertical orientation.
     */
    protected JPanel contentJPanel;
    
    /**
     * The Okay button.
     */
    protected JButton btn_next;
    /**
     * The Cancel button.
     */
    protected JButton btn_cancel;
    /**
     * The Previous button.
     */
    protected JButton btn_prev;
    /**
     * The Finish button.
     */
    protected JButton btn_finish;
    
    /**
     * We will call this object's <code>phasedProcessHandler(...)</code>
     * when the user has finished with this phase.
     * @see PhaseDispatcher#phasedProcessHandler(int, Object)
     */
    private FileFormatDispatcher dispatcher;
    
    /**
     * Creates new PhaseFrame.
     *
     * @param inTitle A short title for the title-bar of this frame.
     * @param inDispatcher The PhasedProcess object in charge of
     * this frame.
     */
    public PhaseFrame(String inTitle) {
        this.setTitle((null != inTitle) ? inTitle : "[Untitled]");
        
        // background panel holds everything
        backgroundPanel = new JPanel();
        backgroundPanel.setLayout(new BorderLayout());
        backgroundPanel.setBorder(new EmptyBorder(5,5,5,5) );  //t,l,r,b
        
        // Create a panel to hold horizontal row of buttons
        navigationPanel = new JPanel();
        navigationPanel.setLayout(new GridBagLayout());
        GridBagConstraints gridBagConstraints1;
        
        btn_cancel = new JButton("Cancel");
        btn_cancel.setMnemonic('C');
        btn_cancel.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                cancelPressed();
            }
        });
        gridBagConstraints1 = new GridBagConstraints();
        gridBagConstraints1.anchor = GridBagConstraints.WEST;
        gridBagConstraints1.weightx = 1.0;
        navigationPanel.add(btn_cancel, gridBagConstraints1);
        
        btn_finish = new JButton("Finish");
        btn_finish.setMnemonic('F');
        btn_finish.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                finishPressed();
            }
        });
        gridBagConstraints1.anchor = GridBagConstraints.EAST;
        gridBagConstraints1.weightx = 1.0;
        navigationPanel.add(btn_finish, gridBagConstraints1);
        
        btn_prev = new JButton("Previous");
        btn_prev.setMnemonic('P');
        btn_prev.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                prevPressed();
            }
        });
        gridBagConstraints1 = new GridBagConstraints();
        gridBagConstraints1.anchor = GridBagConstraints.EAST;
        gridBagConstraints1.weightx = 1.0;
        navigationPanel.add(btn_prev, gridBagConstraints1);
        
        btn_next = new JButton("Next");
        btn_next.setMnemonic('N');
        btn_next.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                nextPressed();
            }
        });
        gridBagConstraints1 = new GridBagConstraints();
        gridBagConstraints1.anchor = GridBagConstraints.EAST;
        gridBagConstraints1.weightx = 0.0;
        navigationPanel.add(btn_next, gridBagConstraints1);
        
        // Stuff in the center of the dialog.
        // This is where subclassers should add their
        // content-specific objects.
        contentJPanel = new JPanel();
		contentJPanel.setLayout(new CardLayout());
	        
        // add stuff to the background hold-all panel
        backgroundPanel.add( contentJPanel, BorderLayout.CENTER);
        backgroundPanel.add( navigationPanel, BorderLayout.SOUTH );
        
        // Add our main panel to the content pane
        Container content = getContentPane();
        content.add(backgroundPanel);
    }
    
    /**
     * Convenience method which shows a JOptionPane dialog
     * with the passed String. This is typically used
     * by <code>validateData</code>.
     * @see #validateData
     */
    protected void badDataAlert( String msg ) {
        if ( null == msg ) {
            msg = "Data validation error.";
        }
        JOptionPane.showMessageDialog(this,
        msg,
        "Alert",
        JOptionPane.ERROR_MESSAGE);
    }
    
    
    /**
     * Implements the action to occur when the <CODE>Next</CODE> button
     * is pressed.
     * <p>
     * The default implementation calls <code>validateData</code>
     * and, if the data is valid, then calls <code>saveData</code>,
     * makes the frame invisible, and calls <code>
     * dispatcher.phasedProcessHandler(SimpleDispatcher.MSG_NEXT, null);</code>
     *
     * @see #validateData
     * @see #saveData
     */
    protected void nextPressed() {
		FormInterface currentPhaseFrame = dispatcher.getCurrentPhaseFrame();
		String msg = currentPhaseFrame.validateData();
        if ( msg != null) {
			displayMessage(msg);
            return;
        }
		currentPhaseFrame.saveData();
		FileFormatDataModel aFileFormatDataModel = currentPhaseFrame.getFileFormatDataModel();
		if(aFileFormatDataModel.isFileDelimited) {
			getDispatcher().setCurrentPath(FileFormatDispatcher.DELIMITED_PATH);
		} else {
			getDispatcher().setCurrentPath(FileFormatDispatcher.FIXED_PATH);
		}
        getDispatcher().phasedProcessHandler(FileFormatDispatcher.MSG_NEXT, null);
		repaint();
    }
    
    /**
     * Implements the action to occur when the Previous button is pressed.
     * The default implementation is:<p><pre>
     *        setVisible(false);
     *        dispatcher.phasedProcessHandler(PhasedProcess.MSG_PREV, null);
     * </pre>
     */
    protected void prevPressed() {
        getDispatcher().phasedProcessHandler(FileFormatDispatcher.MSG_PREV, null);
        repaint();
    }
    
    /**
     * Implements the action to occur when the Cancel button
     * is pressed
     */
    protected void cancelPressed() {
        getDispatcher().phasedProcessHandler(FileFormatDispatcher.MSG_CANCEL, null);
    }
    
    /**
     * Skip to the given phase.
     * @param newPhase value identifying the new phase.
     */
    public void skipToPhase(int newPhase) {
        getDispatcher().phasedProcessHandler(FileFormatDispatcher.MSG_SKIP, new Integer(newPhase));
		repaint();
    }
    
    /**
     * Implements the action to occur when the Finish button
     * is pressed
     */
    protected void finishPressed() {
		FormInterface currentPhasePanel = dispatcher.getCurrentPhaseFrame();

		String msg = currentPhasePanel.validateData();
		if ( msg != null) {
			displayMessage(msg);
			return;
		}
        
		currentPhasePanel.saveData();
        
        getDispatcher().phasedProcessHandler(FileFormatDispatcher.MSG_FINISH, null);
		repaint();
    }
    
    
    /**
     * Convenience method which shows either the standard wait cursor
     * or the standard default cursor.
     *
     * @param waiting If <code>true</code>, the standard wait cursor is shown;
     * if <code>false</code>, the standard default cursor is shown.
     */
    public void setWaitCursor(boolean waiting) {
        setCursor(Cursor.getPredefinedCursor(waiting
        ? Cursor.WAIT_CURSOR
        : Cursor.DEFAULT_CURSOR));
    }
    
    /**
     * Centers this object inside of its parent.
     */
    public void centerFrameInParent() {
        Dimension panelSize = getLayout().preferredLayoutSize(this);
        if ((0 >= panelSize.width) || (0 >= panelSize.height))
            return;
        Insets ins = this.getInsets();
        setSize(new Dimension( panelSize.width + ins.left + ins.right, panelSize.height + ins.top + ins.bottom));
        Dimension screenSize = getParent().getSize();
        if ((0 >= screenSize.width) || (0 >= screenSize.height))
            return;
        Dimension frameSize = getSize();
        if ((0 >= frameSize.width) || (0 >= frameSize.height))
            return;
        frameSize.height = Math.min(frameSize.height, screenSize.height);
        frameSize.width = Math.min(frameSize.width, screenSize.width);
        setSize( frameSize.width, frameSize.height );	// may have been cropped
        setLocation((screenSize.width - frameSize.width) / 2,
        (screenSize.height - frameSize.height) / 2);
    }
    
    /**
     * Subclassers typically put their content in the center of the dialog
     * inside a Box, which is oriented vertically. To abstract this notion,
     * and make things all cozy and encapsulated, subclassers should use
     * this call to get the box they will put their objects into.
     *
     * @return A vertically oriented javax.swing.Box object, which
     * subclassers should use to hold their UI objects.
     */
    public JPanel getContentJPanel() {
        return contentJPanel;
    }
    
	public void showScreen(String panelID) {
		CardLayout cl = (CardLayout)(contentJPanel.getLayout());
		cl.show(contentJPanel, panelID);
	}

	public void addScreen(JPanel aSreen,String panelID) {
		CardLayout cl = (CardLayout)(contentJPanel.getLayout());
		contentJPanel.add(aSreen, panelID);
	}
	    
    /**
     * Return the background JPanel. Though this panel
     * holds everything, most callers will want to
     * use {@link #getContentBox getContentBox}.
     */
    public JPanel getBackgroundPanel() {
        return backgroundPanel;
    }
    
    /**
     * Change the UI so that this frame has the L&amp;F of the first
     * frame&mdash: the <b>Previous</b> button is removed. Trivial,
     * but provides symmetry with finalFrameUI.
     * @see #finalFrameUI()
     */
    public void firstFrameUI() {
        navigationPanel.remove(btn_prev);
		navigationPanel.remove(btn_finish);
		//btn_finish.setEnabled(false);
		navigationPanel.revalidate();
    }

	public void middleFrameUI() {
		navigationPanel.add(btn_prev);
		navigationPanel.add(btn_next);
		btn_next.setEnabled(true);
		btn_prev.setEnabled(true);
		navigationPanel.remove(btn_finish);
		navigationPanel.revalidate();
	}
    
    /**
     * Change the UI so that this frame has the L&amp;F of the final
     * frame&mdash;<b>Next</b> button removed, <b>Finish</b> button
     * on right-hand side of <b>Prev</b> button.
     */
    public void finalFrameUI() {
        // remove all buttons we are concerned with
        navigationPanel.remove(btn_next);
        //navigationPanel.add(btn_prev);
        navigationPanel.add(btn_finish);
		btn_finish.setEnabled(true);
        
        //btn_next = null;    // next button not used in final frame
        navigationPanel.revalidate();
        getRootPane().setDefaultButton(btn_finish);
    }

	public void enableNextButton() {
		btn_next.setEnabled(true);
	}
	
	protected void setDispatcher(FileFormatDispatcher dispatcher) {
		this.dispatcher = dispatcher;
	}

	protected FileFormatDispatcher getDispatcher() {
		return dispatcher;
	}
    
    private void displayMessage(String msg) {
		JOptionPane.showMessageDialog(this, msg);
    }
}
