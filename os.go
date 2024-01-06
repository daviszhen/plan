package main

import "os"

func sqliteOsDelete(string) int                        { return 0 }
func sqliteOsFileExists(string) int                    { return 0 }
func sqliteOsOpenReadWrite(string, *os.File, *int) int { return 0 }
func sqliteOsOpenExclusive(string, *os.File, int) int  { return 0 }
func sqliteOsOpenReadOnly(string, *os.File) int        { return 0 }
func sqliteOsTempFileName(string) int                  { return 0 }
func sqliteOsClose(*os.File) int                       { return 0 }
func sqliteOsRead(*os.File, []byte, int) int           { return 0 }
func sqliteOsWrite(*os.File, []byte, int) int          { return 0 }
func sqliteOsSeek(*os.File, int) int                   { return 0 }
func sqliteOsSync(*os.File) int                        { return 0 }
func sqliteOsTruncate(*os.File, int) int               { return 0 }
func sqliteOsFileSize(*os.File, *int) int              { return 0 }
func sqliteOsReadLock(*os.File) int                    { return 0 }
func sqliteOsWriteLock(*os.File) int                   { return 0 }
func sqliteOsUnlock(*os.File) int                      { return 0 }
func sqliteOsRandomSeed(string) int                    { return 0 }
func sqliteOsSleep(int) int                            { return 0 }
func sqliteOsEnterMutex() int                          { return 0 }
func sqliteOsLeaveMutex() int                          { return 0 }
