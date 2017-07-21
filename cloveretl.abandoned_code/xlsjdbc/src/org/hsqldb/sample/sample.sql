/*
    $Id: sample.sql,v 1.3 2004/07/12 16:51:01 unsaved Exp $
    Examplifies use of SqlTool.
    PCTASK Table creation
*/

/* Ignore error for the two drop statements
 * For HSQLDB databases, you can use "IF EXISTS" instead of ignoring errors:
 *    DROP TABLE x IF EXISTS;
 * "IF EXISTS" is non-portable, however.
 */
\* true
DROP TABLE pctasklist;
DROP TABLE pctask;
\* false

\p Creating table pctask
CREATE TABLE pctask (
    id integer identity,
    name varchar(40),
    description varchar,
    url varchar,
    UNIQUE (name)
);

\p Creating table pctasklist
CREATE TABLE pctasklist (
    id integer identity,
    host varchar(20) not null,
    tasksequence int not null,
    pctask integer,
    assigndate timestamp default current_timestamp,
    completedate timestamp,
    show bit default true,
    FOREIGN KEY (pctask) REFERENCES pctask,
    UNIQUE (host, tasksequence)
);

\p Granting privileges
GRANT select ON pctask TO public;
GRANT all ON pctask TO tomcat;
GRANT select ON pctasklist TO public;
GRANT all ON pctasklist TO tomcat;

\p Inserting test records
INSERT INTO pctask (name, description, url) VALUES (
    'task one', 'Description for task 1', 'http://cnn.com');
INSERT INTO pctasklist (host, tasksequence, pctask) VALUES (
    'admc-masq', 101, SELECT id FROM pctask WHERE name = 'task one');

commit;
