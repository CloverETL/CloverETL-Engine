package org.jetel.util.ddl2clover;

public enum ColumnConstraint {

    NULL(),
    NOT_NULL(),
    PRIMARY_KEY(),
    UNIQUE;
    // CHECK(.., ..)
    // REFERENCES(.., ..)
    
}
