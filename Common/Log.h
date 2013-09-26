#pragma once
#include <stdio.h>

class Log
{
	private:
	static char* logfile;
	static FILE* outputStream;
	static const int MAX_LOG_NAME_SIZE = 50;
	
	public:
	static int sDebugLevel;
	static bool printStats;
	
	Log();
	~Log();

	static void print_log(const char* format, ...);

	static void error_log(const char* format, ...);
	static void info_log(const char* format, ...);
	static void warn_log(const char* format, ...);
	static void debug_log(const char* format, ...);
	static void stats_log(const char* format, ...);

	static void startup_log(const char* format, ...);
	static bool EnableFileLogging(char* file);
	static void RotateLogFile(int sig);
	static long GetLogSize();
	
	private:
	static void print_timestamp(const char* msg);
};

// Debug levels used when logging
enum {
	// Only print critical error messages (nice clean log)
	kOnlyErrors,
	// Print probably harmless warning messages
	kWarnings,
	// Print various informational messages
	kInformational,
	// Print a lot of per event log messages (logs will be huge)
	kFullDebug
};

