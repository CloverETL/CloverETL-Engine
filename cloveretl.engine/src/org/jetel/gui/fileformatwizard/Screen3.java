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
import java.awt.Dimension;
import javax.swing.JLabel;
import java.awt.GridBagConstraints;
import java.awt.Insets;
import javax.swing.JTextPane;
import java.awt.SystemColor;
import java.awt.Font;
import javax.swing.JTable;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.event.TableModelEvent;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;

import org.jetel.gui.component.FormInterface;
import org.jetel.metadata.DataRecordMetadata;

public class Screen3 extends JPanel implements FormInterface {
  private GridBagLayout gridBagLayout1 = new GridBagLayout();
  private JLabel jLabel1 = new JLabel();
  private JLabel jLabel2 = new JLabel();
  private JTextPane jTextPane1 = new JTextPane();
  private JTable jTable1 = null;
  private JPanel jPanel1 = new JPanel();
  private JPanel jPanel2 = new JPanel();
  private GridBagLayout gridBagLayout2 = new GridBagLayout();
  private JLabel jLabel3 = new JLabel();
  private JLabel jLabel4 = new JLabel();
  private JTextField jTextField1 = new JTextField();
  private JCheckBox jCheckBox1 = new JCheckBox();
  private JComboBox jComboBox1 = null;
   private JPanel jPanel3 = new JPanel();
   private JLabel jLabel5 = new JLabel();
   private JLabel jLabel6 = new JLabel();
   private JLabel jLabel7 = new JLabel();
   	 
  private FileFormatDispatcher aDispatcher;
  private FileFormatDataModel aFileFormatDataModel;
  private DataRecordMetadata aDataRecordMetadata;
	
  private int selectedCol = -1;
  
  public Screen3(FileFormatDispatcher aDispatcher, FileFormatDataModel aFileFormatDataModel)
  {
	this.aDispatcher = aDispatcher;
	this.aFileFormatDataModel = aFileFormatDataModel;
  	
    try
    {
		String[] petStrings = { "Bird", "Cat", "Dog", "Rabbit", "Pig" };
		jComboBox1 = new JComboBox(petStrings);
		loadData();
		jbInit();
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }

  }
  public Screen3()
  {
	try
	{
	String[] petStrings = { "Bird", "Cat", "Dog", "Rabbit", "Pig" };
	jComboBox1 = new JComboBox(petStrings);
	Object[][] data = {
		{"Mary", "Campione", 
		 "Snowboarding", new Integer(5), new Boolean(false)},
		{"Alison", "Huml", 
		 "Rowing", new Integer(3), new Boolean(true)},
		{"Kathy", "Walrath",
		 "Chasing toddlers", new Integer(2), new Boolean(false)},
		{"Mark", "Andrews",
		 "Speed reading", new Integer(20), new Boolean(true)},
		{"Angela", "Lih",
		 "Teaching high school", new Integer(4), new Boolean(false)}
	};
	String[] columnNames = null;
	
	columnNames = new String[5];
	columnNames[0]="First Name";
	columnNames[1]="Last Name";
	columnNames[2]="Sport";
	columnNames[3]="# of Years";
	columnNames[4]="Vegetarian";


	  jTable1 = new JTable(data, columnNames);

	  jbInit();
	}
	catch(Exception e)
	{
	  e.printStackTrace();
	}

  }

  public static void main(String[] args)
  {
    Screen3 screen3 = new Screen3();
        JFrame f = new JFrame("RulerPanel");
        f.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e) {System.exit(0);}
        });
        f.getContentPane().add("Center", screen3);
        f.setSize(new Dimension(400,20));
        f.pack();
        f.show();
  }

  private void jbInit() throws Exception
  {
    this.setLayout(gridBagLayout1);
    jLabel1.setText("Screen 3 of 4");
    jLabel1.setFont(new Font("Dialog", 1, 11));
    jLabel2.setText("Specify Field Attributes");
    jTextPane1.setText("Here goes screen help text.");
    jTextPane1.setBackground(SystemColor.control);
    jPanel2.setLayout(gridBagLayout2);
    jLabel3.setText("Field Name");
    jLabel4.setText("Data Type");
    jTextField1.setText("Nothing");
    //jCheckBox1.setText("Is Nullable?");
    
	jLabel6.setText("Nullable?");
	//jPanel3.setBackground(new Color(73, 66, 236));
	jLabel5.setText("is selected for editing");
	jLabel7.setText("Field x");
	//jLabel7.setHorizontalTextPosition(SwingConstants.LEFT);
	//jLabel7.setHorizontalAlignment(SwingConstants.LEFT);
    
	jTextField1.addFocusListener(new java.awt.event.FocusAdapter()
	  {
		public void focusLost(FocusEvent e)
		{
		  jTextField1_focusLost(e);
		}
	  });

	jCheckBox1.addActionListener(new ActionListener()
	  {
		public void actionPerformed(ActionEvent e)
		{
		  jCheckBox1_actionPerformed(e);
		}
	  });

	jComboBox1.addActionListener(new ActionListener()
	  {
		public void actionPerformed(ActionEvent e)
		{
		  jComboBox1_actionPerformed(e);
		}
	  });

    
    
    
	jTable1.setRowSelectionAllowed(false);
	jTable1.setColumnSelectionAllowed(true);
	//jTable1.setCellSelectionEnabled(false);
	jTable1.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
	ListSelectionModel colSM = jTable1.getColumnModel().getSelectionModel();
	colSM.addListSelectionListener(new ListSelectionListener() {
		public void valueChanged(ListSelectionEvent e) {
			//Ignore extra messages.
			if (e.getValueIsAdjusting()) return;

			ListSelectionModel lsm = (ListSelectionModel)e.getSource();
			if (lsm.isSelectionEmpty()) {
				selectedCol = -1;
				jTextField1.setText("");
				jLabel7.setText("Nothing");
				System.out.println("No columns are selected.");
			} else {
				selectedCol = lsm.getMinSelectionIndex();
				System.out.println("Column " + selectedCol
								   + " is now selected.");
				jLabel7.setText(jTable1.getColumnName(selectedCol));
				jTextField1.setText(jTable1.getColumnName(selectedCol));
			}
		}
	});
//	Create the scroll pane and add the table to it. 
	  JScrollPane scrollPane = new JScrollPane(jTable1);

   
    this.add(jLabel1, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.NORTHWEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
    this.add(jLabel2, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    this.add(jTextPane1, new GridBagConstraints(0, 1, 2, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    this.add(scrollPane, new GridBagConstraints(0, 3, 2, 1, 1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
	//this.add(jPanel1, new GridBagConstraints(0, 4, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
	this.add(jPanel2, new GridBagConstraints(0, 2, 2, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
	jPanel2.add(jLabel3, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
	jPanel2.add(jLabel4, new GridBagConstraints(1, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
	jPanel2.add(jTextField1, new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0, GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL, new Insets(0, 5, 5, 5), 50, 0));
	jPanel2.add(jCheckBox1, new GridBagConstraints(2, 2, 1, 1, 0.0, 1.0, GridBagConstraints.NORTH, GridBagConstraints.NONE, new Insets(0, 5, 5, 5), 0, 0));
	jPanel2.add(jComboBox1, new GridBagConstraints(1, 2, 1, 1, 0.0, 0.0, GridBagConstraints.NORTH, GridBagConstraints.NONE, new Insets(0, 5, 5, 5), 0, 0));
	jPanel2.add(jLabel6, new GridBagConstraints(2, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
	jPanel3.add(jLabel7, null);
	jPanel3.add(jLabel5, null);
	jPanel2.add(jPanel3, new GridBagConstraints(0, 0, 3, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
  }

private void jCheckBox1_actionPerformed(ActionEvent e)
{
	Object source = e.getSource();
	System.out.println("ActionEvent e " + jCheckBox1.isSelected());
}

private void jComboBox1_actionPerformed(ActionEvent e)
{
	System.out.println("ActionEvent e " + jComboBox1.getSelectedItem());
}

private void jTextField1_focusLost(FocusEvent e)
{
	System.out.println("FocusEvent e " + " "+ jTextField1.getText());
	if(selectedCol!= -1) { //reset values only if something is selected
		aDataRecordMetadata.getField(selectedCol).setName(jTextField1.getText());
		TableColumnModel aModel= jTable1.getColumnModel();
		TableColumn aTableColumn = aModel.getColumn(selectedCol);
		aTableColumn.setHeaderValue(jTextField1.getText());
		jTable1.setColumnModel(aModel);
		jTable1.tableChanged( new TableModelEvent(jTable1.getModel(),TableModelEvent.HEADER_ROW,TableModelEvent.HEADER_ROW,selectedCol));
	}
}
	/* (non-Javadoc)
	 * @see org.jetel.gui.component.FormInterface#loadData()
	 */
	public void loadData() {
		aDataRecordMetadata = aFileFormatDataModel.recordMeta;
		String[] columnNames = new String[aDataRecordMetadata.getNumFields()];
		for( int i = 0 ; i < columnNames.length ; i++) {
			columnNames[i] = aDataRecordMetadata.getField(i).getName();
		}
		
		Object[][] data = new Object[aFileFormatDataModel.linesFromFile.length ][columnNames.length];
		int fieldSize = 0;
		int fieldStart = 0;
		for( int i = 0 ; i < columnNames.length ; i++) {
			fieldSize = aDataRecordMetadata.getField(i).getSize();
			for(int j=0 ; j<aFileFormatDataModel.linesFromFile.length ; j++) {
				if(aFileFormatDataModel.linesFromFile[j] != null ) {
					data[j][i] = aFileFormatDataModel.linesFromFile[j].substring(fieldStart, fieldSize);
				}
			}
			fieldStart = fieldStart + fieldSize;
		}

		jTable1 = new JTable(data, columnNames);
	}
	
	
	/* (non-Javadoc)
	 * @see org.jetel.gui.component.PhasedPanelInterface#validateData()
	 */
	public boolean validateData() {
		//no validation required; if the user does not make any changes then we just
		//have default values
		return true;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.gui.component.PhasedPanelInterface#saveData()
	 */
	public void saveData() {
		aFileFormatDataModel.recordMeta = aDataRecordMetadata ;
		
	}

}