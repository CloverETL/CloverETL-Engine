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
// FILE: c:/projects/jetel/org/jetel/gui/component/RulerPanel.java
package org.jetel.gui.component;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.geom.*;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.event.MouseEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import javax.swing.JFrame;
import javax.swing.JPanel;


/**
 * This panel supports graphically specifying field widths for fixed-length
 * files.
 *
 * @author   Wes Maciorowski     
 * @since    April 02, 2003
 * @revision    $Revision$
 */
public class RulerPanel extends JPanel {
	private int panelMargin = 7;
	private final static int bigMark = 6;    // size of mark for tens
	private final static int mediumMark = 4; // size of mark for fives
	private final static int smallMark = 2;  // size of mark for ones

	private final static BasicStroke wideStroke = new BasicStroke(8.0f);

	private Dimension totalSize;
	private FontMetrics fontMetrics;

    private String[] linesFromFile = null;

	private HashMap pointList = null;
	private int charWidth = 7;    //will set it in paint()
	private int draggedMark = 0;
	private boolean isDragged = false;

	private int maxUnitIncrement = 1;			//scrolling


	public Object[] getPointList() {
		Object[] retValue = null;
		
		Collection col = pointList.values();
		retValue = col.toArray();
		return retValue;
	}
	/**
	 *  Constructor
	 *
	 *@param  aFont          Font to use to display text in this panel
     *@since                   		April 2, 2003
	 */
  public RulerPanel(Font aFont)
  {
	pointList = new HashMap();
    
    try {
      jbInit(aFont);
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }

  }

  public static void main(String[] args)
  {
    RulerPanel rulerPanel = new RulerPanel(new Font("Courier New", 0, 11));
	String[] linesFromFile = null;
	linesFromFile = new String[5];
	linesFromFile[0] = "2590   189 Alabama Electric Coop Inc          56 Lowman              1  2 S           BIT U   9  KY 225        LODESTAR ENERGY, INC.                        36.174   11939    2   11    132.100 2001";
	linesFromFile[1] = "2591   189 Alabama Electric Coop Inc          56 Lowman              1  2 S           BIT S   13 AL 93         MANN STEEL PRODUCTS, INC.                     8.970   10760    2   18    136.300 2001";
	linesFromFile[2] = "2592   189 Alabama Electric Coop Inc          56 Lowman              1  2 C           BIT S   13 AL 57         PITTSBURG & MIDWAY COAL - BASE                 30.995   12089    2   12    135.300 2001";
	linesFromFile[3] = "2593   189 Alabama Electric Coop Inc          56 Lowman              1  2 C           BIT S   13 AL 57         PITTSBURG & MIDWAY COAL-SUPPLEMENT             18.600   12195    1   12    130.900 2001";
	linesFromFile[4] = "2594   189 Alabama Electric Coop Inc          56 Lowman              1  2 S           FO2     0                MINTO ENERGY CORPORATION                        0.595  130500    0    0    675.700 2001";
	rulerPanel.setLinesFromFile(linesFromFile);
	
        JFrame f = new JFrame("RulerPanel");
        f.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e) {System.exit(0);}
        });
        f.getContentPane().add("Center", rulerPanel);
        f.setSize(new Dimension(400,20));
        f.pack();
        f.show();
  }

  /**
   *  Implements paint method
   *
   *@param  g           Description of Parameter
   *@since              April 2, 2003
   */
    public void paint(Graphics g) {
		super.paint(g);
		
        Graphics2D g2 = (Graphics2D) g;
        g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        Dimension d = getSize();
        int gridWidth = d.width ;
        int gridHeight = d.height ;

		fontMetrics = g2.getFontMetrics();
		int chararacterWidth = fontMetrics.charWidth('0');
		panelMargin = chararacterWidth;
		charWidth = chararacterWidth;

        int charHeight = fontMetrics.getHeight();
        int lineLenght = (linesFromFile == null) ? 120*charWidth : linesFromFile[0].length()*charWidth;
        // int mainLineStartX = panelMargin; 
        int mainLineStartX = panelMargin;
        int textPosY = charHeight + panelMargin;
        int mainLineStartY = textPosY + 1 + bigMark;

        // draw Line2D.Double
        g2.draw(new Line2D.Double(mainLineStartX, mainLineStartY, mainLineStartX+lineLenght * charWidth , mainLineStartY));
        int curX = mainLineStartX;
        String curString = null;
        for( int i = 0 ; i <= lineLenght ; i++ ) 
        {
          if(i%10 == 0 ) 
          {
            curString = Integer.toString(i);
            g2.drawString( curString, curX - (fontMetrics.stringWidth(curString))/2, textPosY);
            g2.draw(new Line2D.Double(curX, mainLineStartY - bigMark, curX , mainLineStartY));
          } else if(i%5 == 0 ) 
          {
            curString = Integer.toString(i);
            //g2.drawString( curString, curX - (fontMetrics.stringWidth(curString))/2, textPosY);
            g2.draw(new Line2D.Double(curX, mainLineStartY - mediumMark, curX , mainLineStartY));
          } else {
            g2.draw(new Line2D.Double(curX, mainLineStartY - smallMark, curX , mainLineStartY));
          }
          curX = curX + charWidth;
        }
		textPosY = mainLineStartY + charHeight + 2;
		// draw white rectangle as a background for test
		g2.setColor(Color.white);
		g2.fill(new Rectangle(mainLineStartX , textPosY - charHeight, lineLenght * charWidth , 5* charHeight+4));
		// display preview lines from file - linesFromFile
		g2.setColor(Color.black);
		if(linesFromFile != null ) {
			for(int i = 0 ; i < linesFromFile.length ; i++) {
				if(linesFromFile[i] != null) {
					g2.drawString( linesFromFile[i], mainLineStartX , textPosY);
					textPosY = textPosY + charHeight;
				}
			}
		}

		// draw marks
		Dimension size = getSize();
		int tmpX = 0;
		Iterator anIterator = pointList.values().iterator();
		while(anIterator.hasNext()) {
			tmpX = ((Integer) anIterator.next()).intValue()*charWidth;
			g.drawLine(tmpX,0,tmpX,gridHeight);
		  }  

		  if(isDragged) {
			g.setColor(Color.red);
			g.drawLine(draggedMark*charWidth,0,draggedMark*charWidth,gridHeight);
		  }
    }

	/**
	 *  Initializes panel
	 *
	 *@param  aFont          Font to use to display text in this panel
	 *@exception  Exception  Description of Exception
     *@since                 April 2, 2003
	 */
  private void jbInit(Font aFont) throws Exception {
    this.setFont(aFont);
	this.setAlignmentX(Component.LEFT_ALIGNMENT);

	int lineLenght = (linesFromFile == null) ? 400 : linesFromFile[0].length()*charWidth;
	lineLenght = lineLenght + 2*panelMargin;
	this.setPreferredSize(new Dimension(lineLenght, 100));
	this.setMinimumSize(new Dimension(lineLenght, 100));
	System.out.println(lineLenght);
	this.addMouseListener(new java.awt.event.MouseAdapter()
	  {
		public void mousePressed(MouseEvent e)
		{
		  this_mousePressed(e);
		}

		public void mouseReleased(MouseEvent e)
		{
		  this_mouseReleased(e);
		}

	  });
	this.addMouseMotionListener(new java.awt.event.MouseMotionAdapter()
	  {
		public void mouseDragged(MouseEvent e)
		{
		  this_mouseDragged(e);
		}

	  });
  }

  /**
   *  mouseDragged handler
   *
   *@param  e           MouseEvent
   *@since              April 2, 2003
   */
  private void this_mouseDragged(MouseEvent e)
  {
	if(isDragged) {
	  draggedMark = findClosestAvailableMarkPosition(e.getX());
	  repaint();
	}
  
  }

  /**
   * Marks can only be placed between characters. So, mouse clicks need to be
   * allocated to potential positions.
   *
   *@param  x           X coord of mouse click
   *@since              April 2, 2003
   */
  private int findClosestAvailableMarkPosition(int x) 
  {
	return Math.round(((float) (x)) / charWidth);
  }
  
  /**
   *  mousePressed handler
   *
   *@param  e           MouseEvent
   *@since                   		April 2, 2003
   */
  private void this_mousePressed(MouseEvent e)
  {
	int tmpX = findClosestAvailableMarkPosition(e.getX());
	if(tmpX*charWidth< panelMargin) return; //field needs to be at least 1char
	isDragged = true;
	draggedMark = tmpX;
	repaint();
  }

  /**
   *  mouseReleased handler
   *
   *@param  e           MouseEvent
   *@since              April 2, 2003
   */
  private void this_mouseReleased(MouseEvent e)
  {
	if(isDragged) {
	  isDragged = false;
	}
  	
	int tmpX = findClosestAvailableMarkPosition(e.getX());
	if(tmpX*charWidth < panelMargin) return; //field needs to be at least 1char

	  Integer tmpInt = new Integer(tmpX);
	  if(pointList.containsKey(tmpInt)) {
		pointList.remove(tmpInt);
	  } else {
		pointList.put(tmpInt,tmpInt);
	  }
	repaint();
  }

  /**
   *  Setter for linesFromFile
   *
   *@param  linesFromFile           A String arrary containing first few lines in the file.
   *@since                   		April 2, 2003
   */
public void setLinesFromFile(String[] linesFromFile) {
	this.linesFromFile = linesFromFile;
	int lineLenght = (linesFromFile == null) ? 120*charWidth : linesFromFile[0].length()*charWidth;
	lineLenght = lineLenght + 4*panelMargin;
	this.setPreferredSize(new Dimension(lineLenght, 100));
	System.out.println(lineLenght);
	this.setMinimumSize(new Dimension(lineLenght, 100));
	repaint();
}

/**
 *  Getter for linesFromFile
 *
 *@return                  linesFromFile	A String arrary.
 *@since                   April 2, 2003
 */
public String[] getLinesFromFile() {
	return linesFromFile;
}

}