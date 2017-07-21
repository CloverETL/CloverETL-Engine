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
package org.jetel.interpreter;

import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

/**
 * @author david
 * @since  2.4.2007
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class TokenManagerHelper implements TransformLangParserConstants {

    Stack<TransformLangParserTokenManager> tokenManagersStack;
    TransformLangParserTokenManager currentManager;
    
    
    TokenManagerHelper(TransformLangParserTokenManager tokenManager){
        tokenManagersStack=new Stack<TransformLangParserTokenManager>();
        currentManager=tokenManager;
    }

    public void addTokenManager(TransformLangParserTokenManager tokenManager) {
        tokenManagersStack.push(currentManager);
        currentManager=tokenManager;
    }
    
    public Token getNextToken() {
        Token token=currentManager.getNextToken();
        while (token.kind==EOF) {
            currentManager=tokenManagersStack.pop();
            if (currentManager==null)
                return token;
            token=currentManager.getNextToken();
        }
        return token;
    }
    
    public void ReInit(JavaCharStream stream) {
        currentManager.ReInit(stream);
    }
    
    public void ReInit(JavaCharStream stream, int lexState) {
        currentManager.ReInit(stream, lexState);
    }
    
    public  void setDebugStream(java.io.PrintStream ds) {
        currentManager.setDebugStream(ds);
    }
    
    public void SwitchTo(int lexState) {
        currentManager.SwitchTo(lexState);
    }
    
    void SkipLexicalActions(Token matchedToken) {
        currentManager.SkipLexicalActions(matchedToken);
    }
}
