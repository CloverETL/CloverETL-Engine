/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Created on Apr 4, 2003
 *  Copyright (C) 2003, 2002  David Pavlis, Wes Maciorowski
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

package org.jetel.gui.fileformatwizard;

import org.jetel.gui.component.FormInterface;


/**
 * A concrete implementation of <code>PhasedProcess</code> which will guide
 * the user through the various phases of Lissajou parameter selection.
 * <p>
 * This object manages the state and transitions between states of the
 * parameters selection process, allowing the user to proceed to the next
 * phase, go back to the previous phase, jump between phases, cancel,
 * and summarily finish the operation.
 *
 * @see PhasedProcess
 */
public class FileFormatDispatcher {
    
	/** Message used to indicate Initialization phase. Value is less than zero. */
	public static final int MSG_INIT = -100;
	/** Message used to indicate Next logical phase. Value is less than zero. */
	public static final int MSG_NEXT = -105;
	/** Message used to indicate Previously visited phase. Value is less than zero. */
	public static final int MSG_PREV = -110;
	/** Message used to indicate a jump to a particular phase. Should be paired with
	 * an <code>Integer</code> value indicating which new phase, for example:<p><pre>
	 *       phasedProcessHandler(PhasedProcess.MSG_SKIP, new Integer(theNewPhase));
	 * </pre>
	 */
	public static final int MSG_SKIP = -115;
	/** Message used to cancellation of entire process. Value is less than zero. */
	public static final int MSG_CANCEL = -120;
	/** Message used to indicate completion of all phases. Value is less than zero. */
	public static final int MSG_FINISH = -125;
    
	/** A constant used to indicate the position of FIXED_PATH screens in wizardPaths. Value is zero. */
	public static final int FIXED_PATH = 0;
	/** A constant used to indicate the position of DELIMITED_PATH screens in wizardPaths. Value is one. */
	public static final int DELIMITED_PATH = 1;

	/**
	 * The current stage of the current Session. Typically
	 * has values like <CODE>STAGE_XXX...</CODE>
	 * The initial value is zero.
	 */
	protected int currentPhase = 0;
	protected int currentPath = 0;
	protected int currentStep = 0;
    
    // phases must begin at zero and increment in units of 1
    public static final int SCREEN1_SELECT_FILE = 0;
    public static final int SCREEN2_PARSE_FIXED = 1;
	public static final int SCREEN3_PARSE_DELIM = 2;
	public static final int SCREEN4_EDIT_FIX_ATT = 3;
	public static final int SCREEN5_EDIT_DEL_ATT = 4;
	public static final int SCREEN6_EDIT_XML = 5;

	protected int[][] wizardPaths = {
		{SCREEN1_SELECT_FILE,
		 SCREEN2_PARSE_FIXED,
		 SCREEN4_EDIT_FIX_ATT,
		 SCREEN6_EDIT_XML},
		{SCREEN1_SELECT_FILE,
		 SCREEN3_PARSE_DELIM,
		 SCREEN5_EDIT_DEL_ATT,
		 SCREEN6_EDIT_XML}};
    
    private FileFormatWizard aFileFormatWizard;
    private FileFormatDataModel parameters;
    private FormInterface[] aFormInterface;
    
    /**
     * Creates new LissajouDispatcher phased process object. 
     * This will retrieve the parameters from the <code>DisplayWindow</code> 
     * instance and clone them for use in the parameter-selection UI.
     * <p>
     * The actual process is begun when <code>beginProcess</code> is called.
     *
     * @see #beginProcess
     * @see PhasedProcess
     */
    public FileFormatDispatcher(FileFormatWizard aFileFormatWizard) {
		setParameters(new FileFormatDataModel());
		aFormInterface = new FormInterface[6];
		this.aFileFormatWizard = aFileFormatWizard;
    }
    
    /**
     * This dispatch method uses <code>currentPhase</code> to handle activity
     * for the current phase of the process.
     *
     * @param data User data for use in the current stage. May
     * safely be <code>null</code>.
     */
    public void performCurrentPhase() {
        switch (currentPhase) {
			case  SCREEN1_SELECT_FILE:
				if(aFormInterface[currentPhase] == null) {
					Screen1 aScreen1 =  new Screen1(this, getParameters());
					aFormInterface[currentPhase] = aScreen1;
					getAFileFormatWizard().addScreen(aScreen1, Integer.toString(currentPhase));
				}
				startPhase(aFormInterface[currentPhase]);
				getAFileFormatWizard().firstFrameUI();  // remove Previous button
				break;
			case  SCREEN2_PARSE_FIXED:
				if(aFormInterface[currentPhase] == null) {
					Screen2f aScreen2f = new Screen2f(this, getParameters());
					aFormInterface[currentPhase] = aScreen2f;
					getAFileFormatWizard().addScreen(aScreen2f, Integer.toString(currentPhase));
				}
				startPhase(aFormInterface[currentPhase]);
				getAFileFormatWizard().middleFrameUI();
				break;
			case  SCREEN3_PARSE_DELIM:
				if(aFormInterface[currentPhase] == null) {
					Screen2d aScreen2d = new Screen2d(this, getParameters());
					aFormInterface[currentPhase] = aScreen2d;
					getAFileFormatWizard().addScreen(aScreen2d, Integer.toString(currentPhase));
				}
				startPhase(aFormInterface[currentPhase]);
				getAFileFormatWizard().middleFrameUI();
				break;
			case  SCREEN4_EDIT_FIX_ATT:
				if(aFormInterface[currentPhase] == null) {
					Screen3 aScreen3 = new Screen3(this, getParameters());
					aFormInterface[currentPhase] = aScreen3;
					getAFileFormatWizard().addScreen(aScreen3, Integer.toString(currentPhase));
				}
				startPhase(aFormInterface[currentPhase]);
			getAFileFormatWizard().middleFrameUI();
				break;
			case  SCREEN5_EDIT_DEL_ATT:
				if(aFormInterface[currentPhase] == null) {
					Screen3 aScreen3 = new Screen3(this, getParameters());
					aFormInterface[currentPhase] = aScreen3;
					getAFileFormatWizard().addScreen(aScreen3, Integer.toString(currentPhase));
				}
				startPhase(aFormInterface[currentPhase]);
				getAFileFormatWizard().middleFrameUI();
				break;
			case  SCREEN6_EDIT_XML:
				if(aFormInterface[currentPhase] == null) {
					Screen4 aScreen4 = new Screen4(this, getParameters());
					aFormInterface[currentPhase] = aScreen4;
					getAFileFormatWizard().addScreen(aScreen4, Integer.toString(currentPhase));
				}
				startPhase(aFormInterface[currentPhase]);
				getAFileFormatWizard().finalFrameUI();
				break;
            default:
                break;
        }
    }
    
	/**
	 * Sets <code>currentPhase</code> to its next value. "advance" is a slight
	 * misnomer since <code>currentPhase</code> can be incremented, decremented,
	 * or, in fact set to any value, with this method. Typically, however,
	 * it is incremented.
	 *
	 * @param msg A value describing special action for the Session Handler.
	 * May be any of:
	 * <ul>
	 *  <li>{@link #MSG_INIT  MSG_INIT}
	 *  <li>{@link #MSG_NEXT  MSG_NEXT}
	 *  <li>{@link #MSG_PREV  MSG_PREV}
	 *  <li>{@link #MSG_SKIP  MSG_SKIP}
	 *  <li>{@link #MSG_CANCEL  MSG_CANCEL}
	 *  <li>{@link #MSG_FINISH  MSG_FINISH}
	 *  </ul><p>Typically, it is MSG_NEXT.
	 *
	 * @param data Any data the message wishes to pass along. May
	 * safely be <code>null</code>.
	 * <p>
	 * <b>Note</b> If the <code>data</code> param is an <code>Integer</code> when
	 * <code>msg</code> is <code>MSG_INIT</code> or <code>MSG_SKIP</code>, then
	 * the value of the <code>data</code> paramter will be used to set
	 * the value of <code>{@link #currentPhase currentPhase}</code>.
	 */
	public void phasedProcessHandler(int msg, Object data) {
		switch(msg) {
			case MSG_INIT:
				// set currentPhase to data value if passed, or set to zero
				if (null != data && data instanceof Integer) {
					currentPhase = ((Integer)data).intValue();
				} else {
					currentPhase = 0;
				}
				break;
			case MSG_NEXT:
				currentStep++;
				currentPhase = wizardPaths[currentPath][currentStep];
				break;
			case MSG_PREV:
				currentStep--;
				currentPhase = wizardPaths[currentPath][currentStep];
				break;
			case MSG_SKIP:
				if (null != data && data instanceof Integer) {
					currentPhase = ((Integer)data).intValue();
				} else {
					System.out.println("Error: phasedProcessHandler got MSG_SKIP with bad phase parameter.");
				}
				break;
			case MSG_FINISH:
				processFinished();
				break;
			case MSG_CANCEL:
				processCancelled();
				break;
			default:
				System.out.println("Error: phasedProcessHandler: Unknown msg: " + msg);
				break; // default
		} // switch(msg)
        
		performCurrentPhase();
        
	}
    
    
	/**
	 * This dispatch method uses <code>currentPhase</code> to switch among one or more
	 * possible operations.
	 * <p>
	 * Typically, the approach  used here looks something like this:
	 * <p><pre><font color="blue">
	 *         switch (currentPhase) {
	 *             case PHASE_ABC:
	 *                 if (null != currentPhaseFrame) {
	 *                     currentPhaseFrame.dispose();
	 *                     currentPhaseFrame = null;
	 *                 }
	 *                 currentPhaseFrame = new ABC_Frame(this);
	 *                 desktopPane.add(currentPhaseFrame, JDesktopPane.DEFAULT_LAYER);
	 *                 currentPhaseFrame.centerFrameInParent();
	 *                 currentPhaseFrame.setVisible(true);
	 *                 break;
	 *             case PHASE_XYZ:
	 *                 <i>[...]</i>
	 *             default:
	 *                 System.out.println("Error: performCurrentPhase: Unknown phase: " + currentPhase);
	 *         }
	 * </font></pre>
	 */
	//abstract public void performCurrentPhase();
    
	/**
	 * Convenience method will dispose of currentPhaseFrame if it exists,
	 * set currentPhaseFrame to newFrame, add it to the desktop, center
	 * it, and show it.
	 */
	protected void startPhase(FormInterface newFrame) {
		newFrame.loadData();
		getAFileFormatWizard().showScreen(Integer.toString(currentPhase));
		
	}
    
    
	/**
	 * Begins the process.
	 * Simply calls: <code>phasedProcessHandler(MSG_INIT, null);</code>
	 */
	public void beginProcess() { phasedProcessHandler(MSG_INIT, null); }
    
	/**
	 * Cancels the current phased process.
	 * Simply calls: <code>phasedProcessHandler(MSG_CANCEL, null);</code>
	 */
	public void cancelProcess() { phasedProcessHandler(MSG_CANCEL, null); }
    
	/**
	 * Finish the current phased process.
	 * Simply calls: <code>phasedProcessHandler(MSG_FINISH, null);</code>
	 */
	public void finishProcess() { phasedProcessHandler(MSG_FINISH, null); }
    
	/**
	 * This method is called when the entire process has been cancelled.
	 * Normally, this occurs when <code>phasedProcessHandler</code>
	 * gets a <code>MSG_CANCEL</code> message.
	 * <p>
	 * Sub-classers may want to override to handle this situation.
	 * The default behavior is to dispose of the currentPhaseFrame
	 * object if it exists, and set the value to <code>null</code>.
	 *
	 * @see #currentPhaseFrame
	 */
	protected void processCancelled() {
		currentPhase = -1;
		aFileFormatWizard.hide();
		System.exit(0);
	}
    
	/**
	 * This method is called when the entire process is complete, either
	 * by naturally moving through all the phases, or possibly by the
	 * user pressing the Finish button.
	 * <p>
	 * Sub-classers may want to override to handle this situation.
	 * The default behavior is to dispose of the currentPhaseFrame
	 * object if it exists, and set the value to <code>null</code>.
	 *
	 * @see #currentPhaseFrame
	 */
	protected void processFinished() {
		currentPhase = -1;
		destroyCurrentPhaseFrame();
	}
    
	/**
	 * Do nothing as it gets hidden but may be used again via previous button.
	 * @see #currentPhaseFrame
	 */
	private void destroyCurrentPhaseFrame() {
	}

	public void setAFileFormatWizard(FileFormatWizard aFileFormatWizard) {
		this.aFileFormatWizard = aFileFormatWizard;
	}

	public FileFormatWizard getAFileFormatWizard() {
		return aFileFormatWizard;
	}

	public void setParameters(FileFormatDataModel parameters) {
		this.parameters = parameters;
	}

	public FileFormatDataModel getParameters() {
		return parameters;
	}

	/**
	 * Returns the currently visible panel
	 * @return currently visible panel
	 */
	public FormInterface getCurrentPhaseFrame() {
		return aFormInterface[currentPhase];
	}

	/**
	 * Allows setting the current Path through wizardPaths array.
	 * @param i
	 */
	public void setCurrentPath(int i) {
		currentPath = i;
	}
    
}
