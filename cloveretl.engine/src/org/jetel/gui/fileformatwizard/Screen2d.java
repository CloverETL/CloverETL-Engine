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
package org.jetel.gui.fileformatwizard;
import javax.swing.JPanel;
import java.awt.GridBagLayout;
import javax.swing.JLabel;
import java.awt.GridBagConstraints;
import java.awt.Insets;
import javax.swing.JTextPane;
import java.awt.Color;
import java.awt.Font;

import javax.swing.JCheckBox;
import javax.swing.JTextField;
import javax.swing.BorderFactory;
import javax.swing.border.EtchedBorder;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.awt.Dimension;

import javax.swing.JFrame;
import javax.swing.JComboBox;
import javax.swing.SwingConstants;

import org.jetel.gui.component.FormInterface;

public class Screen2d extends JPanel implements  FormInterface
{
  private GridBagLayout gridBagLayout1 = new GridBagLayout();
  private JLabel jLabel1 = new JLabel();
  private JLabel jLabel2 = new JLabel();
  private JTextPane jTextPane1 = new JTextPane();
  private JPanel jPanel1 = new JPanel();
  private JPanel DataPreviewPanel = new JPanel();
  private JPanel FillerPanel = new JPanel();
  private GridBagLayout gridBagLayout2 = new GridBagLayout();
  private JCheckBox TabCheckBox = new JCheckBox();
  private JCheckBox SpaceCheckBox = new JCheckBox();
  private JCheckBox SemicolonCheckBox = new JCheckBox();
  private JCheckBox CommaCheckBox = new JCheckBox();
  private JCheckBox OtherCheckBox = new JCheckBox();
  private JTextField OtherTextField = new JTextField();
  private JPanel jPanel2 = new JPanel();
  private JCheckBox ConseqDelimitCheckBox = new JCheckBox();
  private GridBagLayout gridBagLayout3 = new GridBagLayout();
  private JLabel jLabel3 = new JLabel();
  private JComboBox TextQualifierComboBox = new JComboBox();

  private FileFormatDispatcher aDispatcher;
  private FileFormatDataModel aFileFormatParameters;
	
  
  public Screen2d(FileFormatDispatcher aDispatcher, FileFormatDataModel aFileFormatParameters)
  {
	this.aDispatcher = aDispatcher;
	this.aFileFormatParameters = aFileFormatParameters;
  	
    try
    {
      jbInit();
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }

  }
  public Screen2d()
  {
	try
	{
	  jbInit();
	}
	catch(Exception e)
	{
	  e.printStackTrace();
	}

  }

  public static void main(String[] args)
  {
    Screen2d screen2d = new Screen2d();
        JFrame f = new JFrame("RulerPanel");
        f.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e) {System.exit(0);}
        });
        f.getContentPane().add("Center", screen2d);
        f.setSize(new Dimension(400,20));
        f.pack();
        f.show();
  }

  private void jbInit() throws Exception
  {
    this.setLayout(gridBagLayout1);
    this.setSize(new Dimension(441, 300));
    jLabel1.setText("Screen 2 of 4");
    jLabel1.setFont(new Font("Dialog", 1, 11));
    jLabel2.setText("Specify Field Delimiters");
    jLabel2.setHorizontalTextPosition(SwingConstants.CENTER);
    jLabel2.setHorizontalAlignment(SwingConstants.CENTER);
    jTextPane1.setText("Here goes screen help text.");
    jPanel1.setLayout(gridBagLayout2);
    jPanel1.setBorder(BorderFactory.createEtchedBorder(EtchedBorder.LOWERED));
    DataPreviewPanel.setBackground(new Color(68, 189, 236));
    TabCheckBox.setText("Tab");
    TabCheckBox.setActionCommand("TabCheckBox");
    SpaceCheckBox.setText("Space");
    SemicolonCheckBox.setText("Semicolon");
    CommaCheckBox.setText("Comma");
    OtherCheckBox.setText("Other");
    jPanel2.setLayout(gridBagLayout3);
    ConseqDelimitCheckBox.setText("Treat consecutive delimiters as one.");
    jLabel3.setText("Text Qualifier");
    this.add(jLabel1, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
    this.add(jLabel2, new GridBagConstraints(1, 0, 2, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    this.add(jTextPane1, new GridBagConstraints(0, 1, 3, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    jPanel1.add(TabCheckBox, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
    jPanel1.add(SpaceCheckBox, new GridBagConstraints(2, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
    jPanel1.add(SemicolonCheckBox, new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
    jPanel1.add(CommaCheckBox, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
    jPanel1.add(OtherCheckBox, new GridBagConstraints(1, 1, 2, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
    jPanel1.add(OtherTextField, new GridBagConstraints(2, 1, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 20, 0));
    this.add(jPanel1, new GridBagConstraints(0, 2, 2, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
    this.add(DataPreviewPanel, new GridBagConstraints(0, 3, 3, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 20));
    this.add(FillerPanel, new GridBagConstraints(2, 4, 3, 1, 1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    jPanel2.add(ConseqDelimitCheckBox, new GridBagConstraints(0, 0, 3, 1, 1.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    jPanel2.add(jLabel3, new GridBagConstraints(1, 1, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
    jPanel2.add(TextQualifierComboBox, new GridBagConstraints(2, 1, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
    this.add(jPanel2, new GridBagConstraints(2, 2, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, -7));

  }

/* (non-Javadoc)
 * @see org.jetel.gui.component.PhasedPanelInterface#validateData()
 */
public boolean validateData() {
	// TODO Auto-generated method stub
	return false;
}

/* (non-Javadoc)
 * @see org.jetel.gui.component.PhasedPanelInterface#saveData()
 */
public void saveData() {
	// TODO Auto-generated method stub
	
}
/* (non-Javadoc)
 * @see org.jetel.gui.component.FormInterface#loadData()
 */
public void loadData() {
	// TODO Auto-generated method stub
	
}
}