/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP, 
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, 
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals 
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2004, The HSQL Development Group
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


package org.hsqldb;

import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.Iterator;
import org.hsqldb.HsqlNameManager.HsqlName;

// fredt@users 20020225 - patch 1.7.0 by boucherb@users - named constraints
// fredt@users 20020320 - doc 1.7.0 - update
// tony_lai@users 20020820 - patch 595156 - violation of Integrity constraint name

/**
 * Implementation of a table constraint with references to the indexes used
 * by the constraint.<p>
 *
 * @version    1.7.2
 */
class Constraint {

    /*
     SQL CLI codes

     Referential Constraint 0 CASCADE
     Referential Constraint 1 RESTRICT
     Referential Constraint 2 SET NULL
     Referential Constraint 3 NO ACTION
     Referential Constraint 4 SET DEFAULT
     */
    static final int CASCADE     = 0,
                     SET_NULL    = 2,
                     NO_ACTION   = 3,
                     SET_DEFAULT = 4;
    static final int FOREIGN_KEY = 0,
                     MAIN        = 1,
                     UNIQUE      = 2,
                     CHECK       = 3;
    ConstraintCore   core;
    HsqlName         constName;
    int              constType;

    /**
     *  Constructor declaration for UNIQUE
     */
    Constraint(HsqlName name, Table t, Index index) {

        core           = new ConstraintCore();
        constName      = name;
        constType      = UNIQUE;
        core.mainTable = t;
        core.mainIndex = index;
        /* fredt - in unique constraints column list for iColMain is the
           visible columns of iMain
         */
        core.mainColArray = ArrayUtil.arraySlice(index.getColumns(), 0,
                index.getVisibleColumns());
        core.colLen = core.mainColArray.length;
    }

    /**
     *  Constructor for main constraints (foreign key references in PK table)
     */
    Constraint(HsqlName name, Constraint fkconstraint) {

        constName = name;
        constType = MAIN;
        core      = fkconstraint.core;
    }

    /**
     *  Constructor for foreign key constraints.
     *
     * @param  pkname name in the main (referenced) table, used internally
     * @param  fkname name in the referencing table, public name of the constraint
     * @param  main referenced table
     * @param  ref referencing talbe
     * @param  colmain array of column indexes in main table
     * @param  colref array of column indexes in referencing table
     * @param  imain index on the main table
     * @param  iref index on the referencing table
     * @param  deleteAction triggered action on delete
     * @param  updateAction triggered action on update
     * @exception  HsqlException
     */
    Constraint(HsqlName pkname, HsqlName fkname, Table main, Table ref,
               int colmain[], int colref[], Index imain, Index iref,
               int deleteAction, int updateAction) throws HsqlException {

        core           = new ConstraintCore();
        core.pkName    = pkname;
        core.fkName    = fkname;
        constName      = fkname;
        constType      = FOREIGN_KEY;
        core.mainTable = main;
        core.refTable  = ref;
        /* fredt - in FK constraints column lists for iColMain and iColRef have
           identical sets to visible columns of iMain and iRef respectively
           but the order of columns can be different and must be preserved
         */
        core.mainColArray = colmain;
        core.colLen       = core.mainColArray.length;
        core.refColArray  = colref;
        core.mainIndex    = imain;
        core.refIndex     = iref;
        core.deleteAction = deleteAction;
        core.updateAction = updateAction;
    }

    /**
     * temp constraint constructor
     */
    Constraint(HsqlName name, int[] mainCol, Table refTable, int[] refCol,
               int type, int deleteAction, int updateAction) {

        core              = new ConstraintCore();
        constName         = name;
        constType         = type;
        core.mainColArray = mainCol;
        core.refTable     = refTable;
        core.refColArray  = refCol;
        core.deleteAction = deleteAction;
        core.updateAction = updateAction;
    }

    private Constraint() {}

    /**
     * Returns the HsqlName.
     */
    HsqlName getName() {
        return constName;
    }

    /**
     * Changes constraint name.
     */
    private void setName(String name, boolean isquoted) throws HsqlException {
        constName.rename(name, isquoted);
    }

    /**
     *  probably a misnomer, but DatabaseMetaData.getCrossReference specifies
     *  it this way (I suppose because most FKs are declared against the PK of
     *  another table)
     *
     *  @return name of the index refereneced by a foreign key
     */
    String getPkName() {
        return core.pkName == null ? null
                                   : core.pkName.name;
    }

    /**
     *  probably a misnomer, but DatabaseMetaData.getCrossReference specifies
     *  it this way (I suppose because most FKs are declared against the PK of
     *  another table)
     *
     *  @return name of the index for the referencing foreign key
     */
    String getFkName() {
        return core.fkName == null ? null
                                   : core.fkName.name;
    }

    /**
     *  Returns the type of constraint
     */
    int getType() {
        return constType;
    }

    /**
     *  Returns the main table
     */
    Table getMain() {
        return core.mainTable;
    }

    /**
     *  Returns the main index
     */
    Index getMainIndex() {
        return core.mainIndex;
    }

    /**
     *  Returns the reference table
     */
    Table getRef() {
        return core.refTable;
    }

    /**
     *  Returns the reference index
     */
    Index getRefIndex() {
        return core.refIndex;
    }

    /**
     *  The ON DELETE triggered action of (foreign key) constraint
     */
    int getDeleteAction() {
        return core.deleteAction;
    }

    /**
     *  The ON UPDATE triggered action of (foreign key) constraint
     */
    int getUpdateAction() {
        return core.updateAction;
    }

    /**
     *  Returns the main table column index array
     */
    int[] getMainColumns() {
        return core.mainColArray;
    }

    /**
     *  Returns the reference table column index array
     */
    int[] getRefColumns() {
        return core.refColArray;
    }

    /**
     *  Returns true if an index is part this constraint and the constraint is set for
     *  a foreign key. Used for tests before dropping an index.
     */
    boolean isIndexFK(Index index) {

        if (constType == FOREIGN_KEY || constType == MAIN) {
            if (core.mainIndex == index || core.refIndex == index) {
                return true;
            }
        }

        return false;
    }

    /**
     *  Returns true if an index is part this constraint and the constraint is set for
     *  a unique constraint. Used for tests before dropping an index.
     */
    boolean isIndexUnique(Index index) {
        return (constType == UNIQUE && core.mainIndex == index);
    }

    /**
     * Only for check constraints
     */
    boolean hasColumn(Table table, String colname) {

        if (constType != CHECK) {
            return false;
        }

        Expression.Collector coll = new Expression.Collector();

        coll.addAll(core.check, Expression.COLUMN);

        Iterator it = coll.iterator();

        for (; it.hasNext(); ) {
            Expression e = (Expression) it.next();

            if (e.getColumnName().equals(colname)
                    && table.tableName.name.equals(e.getTableName())) {
                return true;
            }
        }

        return false;
    }

// fredt@users 20020225 - patch 1.7.0 by fredt - duplicate constraints

    /**
     * Compares this with another constraint column set. This implementation
     * only checks UNIQUE constraints.
     */
    boolean isEquivalent(int col[], int type) {

        if (type != constType || constType != UNIQUE
                || core.colLen != col.length) {
            return false;
        }

        return ArrayUtil.haveEqualSets(core.mainColArray, col, core.colLen);
    }

    /**
     * Compares this with another constraint column set. This implementation
     * only checks FOREIGN KEY constraints.
     */
    boolean isEquivalent(Table tablemain, int colmain[], Table tableref,
                         int colref[]) {

        if (constType != Constraint.MAIN
                || constType != Constraint.FOREIGN_KEY) {
            return false;
        }

        if (tablemain != core.mainTable || tableref != core.refTable) {
            return false;
        }

        return ArrayUtil.areEqualSets(core.mainColArray, colmain)
               && ArrayUtil.areEqualSets(core.refColArray, colref);
    }

    /**
     *  Used to update constrains to reflect structural changes in a table.
     *  Prior checks must ensure that this method does not throw.
     *
     * @param  oldt reference to the old version of the table
     * @param  newt referenct to the new version of the table
     * @param  colindex index at which table column is added or removed
     * @param  adjust -1, 0, +1 to indicate if column is added or removed
     * @throws  HsqlException
     */
    void replaceTable(Table oldt, Table newt, int colindex,
                      int adjust) throws HsqlException {

        if (oldt == core.mainTable) {
            core.mainTable = newt;

            // excluede CHECK
            if (core.mainIndex != null) {
                core.mainIndex =
                    core.mainTable.getIndex(core.mainIndex.getName().name);
                core.mainColArray =
                    ArrayUtil.toAdjustedColumnArray(core.mainColArray,
                                                    colindex, adjust);
            }
        }

        if (oldt == core.refTable) {
            core.refTable = newt;

            if (core.refIndex != null) {
                core.refIndex =
                    core.refTable.getIndex(core.refIndex.getName().name);

                if (core.refIndex != core.mainIndex) {
                    core.refColArray =
                        ArrayUtil.toAdjustedColumnArray(core.refColArray,
                                                        colindex, adjust);
                }
            }
        }
    }

    /**
     * Checks for foreign key or check constraint violation when
     * inserting a row into the child table.
     */
    void checkInsert(Session session, Object row[]) throws HsqlException {

        if (constType == Constraint.MAIN || constType == Constraint.UNIQUE) {

            // inserts in the main table are never a problem
            // unique constraints are checked by the unique index
            return;
        }

        if (constType == Constraint.CHECK) {
            checkCheckConstraint(session, row);

            return;
        }

        if (Index.isNull(row, core.refColArray)) {
            return;
        }

        // a record must exist in the main table
        if (core.mainIndex.findNotNull(row, core.refColArray, true) == null) {

            // special case: self referencing table and self referencing row
            if (core.mainTable == core.refTable) {
                boolean match = true;

                for (int i = 0; i < core.colLen; i++) {
                    if (!row[core.refColArray[i]].equals(
                            row[core.mainColArray[i]])) {
                        match = false;

                        break;
                    }
                }

                if (match) {
                    return;
                }
            }

            throw Trace.error(Trace.INTEGRITY_CONSTRAINT_VIOLATION_NOPARENT,
                              Trace.Constraint_violation, new Object[] {
                core.fkName.name, core.mainTable.getName().name
            });
        }
    }

    /*
     * Tests a row against this CHECK constraint.
     */
    void checkCheckConstraint(Session session,
                              Object[] row) throws HsqlException {

        core.checkFilter.currentData = row;

        if (Boolean.FALSE.equals(core.check.test(session))) {
            core.checkFilter.currentRow = null;

            throw Trace.error(Trace.CHECK_CONSTRAINT_VIOLATION,
                              Trace.Constraint_violation, new Object[] {
                constName.name, core.mainTable.tableName.name
            });
        }

        core.checkFilter.currentData = null;

        return;
    }

// fredt@users 20020225 - patch 1.7.0 - cascading deletes

    /**
     * New method to find any referencing node (containing the row) for a
     * foreign key (finds row in child table). If ON DELETE CASCADE is
     * supported by this constraint, then the method finds the first row
     * among the rows of the table ordered by the index and doesn't throw.
     * Without ON DELETE CASCADE, the method attempts to finds any row that
     * exists, in which case it throws an exception. If no row is found,
     * null is returned.
     * (fredt@users)
     *
     * @param  row array of objects for a database row
     * @param  forDelete should we allow 'ON DELETE CASCADE' or 'ON UPDATE CASCADE'
     * @return Node object or null
     * @throws  HsqlException
     */
    Node findFkRef(Object row[], boolean forDelete) throws HsqlException {

        if (row == null) {
            return null;
        }

        if (Index.isNull(row, core.mainColArray)) {
            return null;
        }

        // there must be no record in the 'slave' table
        // sebastian@scienion -- dependent on forDelete | forUpdate
        boolean findfirst = forDelete ? core.deleteAction != NO_ACTION
                                      : core.updateAction != NO_ACTION;
        Node node = core.refIndex.findNotNull(row, core.mainColArray,
                                              findfirst);

        // tony_lai@users 20020820 - patch 595156
        // sebastian@scienion -- check whether we should allow 'ON DELETE CASCADE' or 'ON UPDATE CASCADE'
        if (!(node == null || findfirst)) {
            throw Trace.error(Trace.INTEGRITY_CONSTRAINT_VIOLATION,
                              Trace.Constraint_violation, new Object[] {
                core.fkName.name, core.refTable.getName().name
            });
        }

        return node;
    }

    /**
     * For the candidate table row, finds any referring node in the main table.
     * This is used to check referential integrity when updating a node. We
     * have to make sure that the main table still holds a valid main record.
     * If a valid row is found the corresponding <code>Node</code> is returned.
     * Otherwise a 'INTEGRITY VIOLATION' Exception gets thrown.
     */
    Node findMainRef(Object row[]) throws HsqlException {

        if (Index.isNull(row, core.refColArray)) {
            return null;
        }

        Node node = core.mainIndex.findNotNull(row, core.refColArray, true);

        // -- there has to be a valid node in the main table
        // --
        if (node == null) {
            throw Trace.error(Trace.INTEGRITY_CONSTRAINT_VIOLATION_NOPARENT,
                              Trace.Constraint_violation, new Object[] {
                core.fkName.name, core.refTable.getName().name
            });
        }

        return node;
    }

    /**
     * Test used before adding a new foreign key constraint. This method
     * returns true if the given row has a corresponding row in the main
     * table. Also returns true if any column covered by the foreign key
     * constraint has a null value.
     */
    private static boolean hasReferencedRow(Object[] rowdata,
            int[] rowColArray, Index mainIndex) throws HsqlException {

        // check for nulls and return true if any
        for (int i = 0; i < rowColArray.length; i++) {
            Object o = rowdata[rowColArray[i]];

            if (o == null) {
                return true;
            }
        }

        // else a record must exist in the main index
        if (mainIndex.find(rowdata, rowColArray) == null) {
            return false;
        }

        return true;
    }

    /**
     * Check used before creating a new foreign key cosntraint, this method
     * checks all rows of a table to ensure they all have a corresponding
     * row in the main table.
     */
    static void checkReferencedRows(Table table, int[] rowColArray,
                                    Index mainIndex) throws HsqlException {

        Index index = table.getPrimaryIndex();
        Node  node  = index.first();

        while (node != null) {
            Object[] rowdata = node.getData();

            if (!Constraint.hasReferencedRow(rowdata, rowColArray,
                                             mainIndex)) {
                String colvalues = "";

                for (int i = 0; i < rowColArray.length; i++) {
                    Object o = rowdata[rowColArray[i]];

                    colvalues += o;
                    colvalues += ",";
                }

                throw Trace.error(
                    Trace.INTEGRITY_CONSTRAINT_VIOLATION_NOPARENT,
                    Trace.Constraint_violation, new Object[] {
                    colvalues, table.getName().name
                });
            }

            node = index.next(node);
        }
    }
}
