/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-07  David Pavlis <david.pavlis@centrum.cz> and others.
 *    
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *    
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
 *    Lesser General Public License for more details.
 *    
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Created on 2.4.2007
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.interpreter.extensions;

import org.jetel.data.primitive.CloverDouble;
import org.jetel.data.primitive.Numeric;
import org.jetel.interpreter.Stack;
import org.jetel.interpreter.TransformLangExecutorRuntimeException;
import org.jetel.interpreter.data.TLContext;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.util.StringUtils;

public class MathLib {

    public static void init() {

        // SQRT

        TLFunctionFactory.registerFunction(new TLFunctionPrototype("math",
                "sqrt", new TLValueType[] { TLValueType.DECIMAL },
                TLValueType.DOUBLE) {
            @Override
            public TLValue execute(TLValue[] params, TLContext context) {
                if (params[0].type.isNumeric()) {
                    TLValue retVal;
                    try {
                        retVal = new TLValue(TLValueType.DOUBLE,
                                new CloverDouble(Math.sqrt(params[0]
                                        .getDouble())));
                        return retVal;
                    } catch (Exception ex) {
                        throw new TransformLangExecutorRuntimeException(
                                "Error when executing SQRT function", ex);
                    }
                }
                throw new TransformLangExecutorRuntimeException(null,
                        params, "sqrt - wrong type of literal(s)");

            }
        });
        
        // LOG

        TLFunctionFactory.registerFunction(new TLFunctionPrototype("math",
                "log", new TLValueType[] { TLValueType.DECIMAL },
                TLValueType.DOUBLE) {
            @Override
            public TLValue execute(TLValue[] params, TLContext context) {
                if (params[0].type.isNumeric()) {
                    TLValue retVal;
                    try {
                        retVal = new TLValue(TLValueType.DOUBLE,
                                new CloverDouble(Math.log((params[0]
                                        .getDouble()))));
                        return retVal;
                    } catch (Exception ex) {
                        throw new TransformLangExecutorRuntimeException(
                                "Error when executing LOG function", ex);
                    }
                }
                throw new TransformLangExecutorRuntimeException(null,
                        params, "log - wrong type of literal(s)");

            }
        });

        //  LOG10

        TLFunctionFactory.registerFunction(new TLFunctionPrototype("math",
                "log10", new TLValueType[] { TLValueType.DECIMAL },
                TLValueType.DOUBLE) {
            @Override
            public TLValue execute(TLValue[] params, TLContext context) {
                if (params[0].type.isNumeric()) {
                    TLValue retVal;
                    try {
                        retVal = new TLValue(TLValueType.DOUBLE,
                                new CloverDouble(Math.log10((params[0]
                                        .getDouble()))));
                        return retVal;
                    } catch (Exception ex) {
                        throw new TransformLangExecutorRuntimeException(
                                "Error when executing LOG10 function", ex);
                    }
                }
                throw new TransformLangExecutorRuntimeException(null,
                        params, "log10 - wrong type of literal(s)");

            }
        });
     
        //  EXP

        TLFunctionFactory.registerFunction(new TLFunctionPrototype("math",
                "exp", new TLValueType[] { TLValueType.DECIMAL },
                TLValueType.DOUBLE) {
            @Override
            public TLValue execute(TLValue[] params, TLContext context) {
                if (params[0].type.isNumeric()) {
                    TLValue retVal;
                    try {
                        retVal = new TLValue(TLValueType.DOUBLE,
                                new CloverDouble(Math.exp((params[0]
                                        .getDouble()))));
                        return retVal;
                    } catch (Exception ex) {
                        throw new TransformLangExecutorRuntimeException(
                                "Error when executing EXP function", ex);
                    }
                }
                throw new TransformLangExecutorRuntimeException(null,
                        params, "exp - wrong type of literal(s)");

            }
        });                        

        // ROUND

        TLFunctionFactory.registerFunction(new TLFunctionPrototype("math",
                "round", new TLValueType[] { TLValueType.DECIMAL },
                TLValueType.DOUBLE) {
            @Override
            public TLValue execute(TLValue[] params, TLContext context) {
                if (params[0].type.isNumeric()) {
                    TLValue retVal;
                    try {
                        retVal = new TLValue(TLValueType.DOUBLE,
                                new CloverDouble(Math.round((params[0]
                                        .getDouble()))));
                        return retVal;
                    } catch (Exception ex) {
                        throw new TransformLangExecutorRuntimeException(
                                "Error when executing ROUND function", ex);
                    }
                }
                throw new TransformLangExecutorRuntimeException(null,
                        params, "round - wrong type of literal(s)");

            }
        });                        
        
        // POW

        TLFunctionFactory.registerFunction(new TLFunctionPrototype("math",
                "pow", new TLValueType[] { TLValueType.DECIMAL, TLValueType.DECIMAL },
                TLValueType.DOUBLE) {
            @Override
            public TLValue execute(TLValue[] params, TLContext context) {
                if (params[0].type.isNumeric()) {
                    TLValue retVal;
                    try {
                        retVal = new TLValue(TLValueType.DOUBLE,
                                new CloverDouble(Math.pow(params[0].getDouble(),params[1].getDouble())));
                        return retVal;
                    } catch (Exception ex) {
                        throw new TransformLangExecutorRuntimeException(
                                "Error when executing POW function", ex);
                    }
                }
                throw new TransformLangExecutorRuntimeException(null,
                        params, "pow - wrong type of literal(s)");

            }
        });                        
        
        // PI

        TLFunctionFactory.registerFunction(new TLFunctionPrototype("math",
                "pi", new TLValueType[] { },
                TLValueType.DOUBLE) {
            @Override
            public TLValue execute(TLValue[] params, TLContext context) {
                if (params[0].type.isNumeric()) {
                    TLValue retVal;
                    try {
                        retVal = new TLValue(TLValueType.DOUBLE,
                                new CloverDouble(Math.PI));
                        return retVal;
                    } catch (Exception ex) {
                        throw new TransformLangExecutorRuntimeException(
                                "Error when executing PI function", ex);
                    }
                }
                throw new TransformLangExecutorRuntimeException(null,
                        params, "pi - wrong type of literal(s)");

            }
        });          

        //  RANDOM

        TLFunctionFactory.registerFunction(new TLFunctionPrototype("math",
                "random", new TLValueType[] { },
                TLValueType.DOUBLE) {
            @Override
            public TLValue execute(TLValue[] params, TLContext context) {
                if (params[0].type.isNumeric()) {
                    TLValue retVal;
                    try {
                        retVal = new TLValue(TLValueType.DOUBLE,
                                new CloverDouble(Math.random()));
                        return retVal;
                    } catch (Exception ex) {
                        throw new TransformLangExecutorRuntimeException(
                                "Error when executing RANDOM function", ex);
                    }
                }
                throw new TransformLangExecutorRuntimeException(null,
                        params, "random - wrong type of literal(s)");

            }
        });                        

        
    }
}
