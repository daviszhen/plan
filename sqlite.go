package main

const (
	/*
	 ** Return values for sqlite_exec()
	 */
	SQLITE_OK         = 0  /* Successful result */
	SQLITE_ERROR      = 1  /* SQL error or missing database */
	SQLITE_INTERNAL   = 2  /* An internal logic error in SQLite */
	SQLITE_PERM       = 3  /* Access permission denied */
	SQLITE_ABORT      = 4  /* Callback routine requested an abort */
	SQLITE_BUSY       = 5  /* The database file is locked */
	SQLITE_LOCKED     = 6  /* A table in the database is locked */
	SQLITE_NOMEM      = 7  /* A malloc() failed */
	SQLITE_READONLY   = 8  /* Attempt to write a readonly database */
	SQLITE_INTERRUPT  = 9  /* Operation terminated by sqlite_interrupt() */
	SQLITE_IOERR      = 10 /* Some kind of disk I/O error occurred */
	SQLITE_CORRUPT    = 11 /* The database disk image is malformed */
	SQLITE_NOTFOUND   = 12 /* (Internal Only) Table or record not found */
	SQLITE_FULL       = 13 /* Insertion failed because database is full */
	SQLITE_CANTOPEN   = 14 /* Unable to open the database file */
	SQLITE_PROTOCOL   = 15 /* Database lock protocol error */
	SQLITE_EMPTY      = 16 /* (Internal Only) Database table is empty */
	SQLITE_SCHEMA     = 17 /* The database schema changed */
	SQLITE_TOOBIG     = 18 /* Too much data for one row of a table */
	SQLITE_CONSTRAINT = 19 /* Abort due to contraint violation */
	SQLITE_MISMATCH   = 20 /* Data type mismatch */
	SQLITE_MISUSE     = 21 /* Library used incorrectly */

)
