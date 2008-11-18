/* Copyright (c) 2001-2004, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG, 
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, 
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb.util;

import java.awt.Color;
import java.awt.Image;
import java.awt.SystemColor;

import javax.swing.ImageIcon;
import javax.swing.UIDefaults;
import javax.swing.UIManager;

// sqlbob@users 20020407 - patch 1.7.0 - reengineering

/**
 * Common code in the Swing versions of DatabaseManager and Tranfer
 * @version 1.7.0
 */
class CommonSwing {

    // (ulrivo): An actual icon.
    static Image getIcon() {
        return (new ImageIcon(
            CommonSwing.class.getResource("hsqldb.gif")).getImage());
    }

    static void setDefaultColor() {

        Color      hsqlBlue  = new Color(102, 153, 204);
        Color      hsqlGreen = new Color(153, 204, 204);
        UIDefaults d         = UIManager.getLookAndFeelDefaults();

        d.put("MenuBar.background", SystemColor.control);
        d.put("Menu.background", SystemColor.control);
        d.put("Menu.selectionBackground", hsqlBlue);
        d.put("MenuItem.background", SystemColor.menu);
        d.put("MenuItem.selectionBackground", hsqlBlue);
        d.put("Separator.foreground", SystemColor.controlDkShadow);
        d.put("Button.background", SystemColor.control);
        d.put("CheckBox.background", SystemColor.control);
        d.put("Label.background", SystemColor.control);
        d.put("Label.foreground", Color.black);
        d.put("Panel.background", SystemColor.control);
        d.put("PasswordField.selectionBackground", hsqlGreen);
        d.put("PasswordField.background", SystemColor.white);
        d.put("TextArea.selectionBackground", hsqlGreen);
        d.put("TextField.background", SystemColor.white);
        d.put("TextField.selectionBackground", hsqlGreen);
        d.put("TextField.background", SystemColor.white);
        d.put("ScrollBar.background", SystemColor.controlHighlight);
        d.put("ScrollBar.foreground", SystemColor.control);
        d.put("ScrollBar.track", SystemColor.controlHighlight);
        d.put("ScrollBar.trackHighlight", SystemColor.controlDkShadow);
        d.put("ScrollBar.thumb", SystemColor.control);
        d.put("ScrollBar.thumbHighlight", SystemColor.controlHighlight);
        d.put("ScrollBar.thumbDarkShadow", SystemColor.controlDkShadow);
        d.put("ScrollBar.thumbLightShadow", SystemColor.controlShadow);
        d.put("ComboBox.background", SystemColor.control);
        d.put("ComboBox.selectionBackground", hsqlBlue);
        d.put("Table.background", SystemColor.white);
        d.put("Table.selectionBackground", hsqlBlue);
        d.put("TableHeader.background", SystemColor.control);

        // This doesn't seem to work.
        d.put("SplitPane.background", SystemColor.control);
        d.put("Tree.selectionBackground", hsqlBlue);
        d.put("List.selectionBackground", hsqlBlue);
    }

    private CommonSwing() {}
}
