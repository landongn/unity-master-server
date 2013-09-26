#include "Log.h"
#include <time.h>
#include <stdio.h>
#include <stdarg.h>

int Log::sDebugLevel = kFullDebug;
FILE* Log::outputStream = stdout;
char* Log::logfile;
bool Log::printStats;

Log::~Log()
{
	if (outputStream)
		fclose(outputStream);
}

void Log::print_timestamp(const char* msg)
{
	if (!outputStream)
		return;
	time_t rawtime;
	struct tm * timeinfo;
	time ( &rawtime );
	timeinfo = localtime ( &rawtime );
	fprintf(outputStream, "%02d-%02d-%d %02d:%02d:%02d\t%s\t",timeinfo->tm_mday, 1+timeinfo->tm_mon, 1900+timeinfo->tm_year, timeinfo->tm_hour, timeinfo->tm_min, timeinfo->tm_sec, msg);
}

void Log::print_log(const char* format, ...)
{
	if (!outputStream)
		return;
	print_timestamp("LOG");;	
	va_list va;
	va_start( va, format );
	vfprintf(outputStream, format, va);
}

void Log::warn_log(const char* format, ...)
{
	if (sDebugLevel >= kWarnings)
	{
		if (!outputStream)
			return;
		print_timestamp("WARN");;
		va_list va;
		va_start( va, format );
		vfprintf(outputStream, format, va);
	}
}

void Log::info_log(const char* format, ...)
{
	if (sDebugLevel >= kInformational)
	{
		if (!outputStream)
			return;
		print_timestamp("INFO");;
		va_list va;
		va_start( va, format );
		vfprintf(outputStream, format, va);
	}
}

void Log::debug_log(const char* format, ...)
{
	if (sDebugLevel >= kFullDebug)
	{
		if (!outputStream)
			return;
		print_timestamp("DEBUG");
		va_list va;
		va_start( va, format );
		vfprintf(outputStream, format, va);
	}
}

void Log::stats_log(const char* format, ...)
{
	if (sDebugLevel >= kOnlyErrors && printStats)
	{
		if (!outputStream)
			return;
		print_timestamp("STATS");		
		va_list va;
		va_start( va, format );
		vfprintf(outputStream, format, va);
	}
}


void Log::startup_log(const char* format, ...)
{
	if (!outputStream)
		return;
	print_timestamp("");
	va_list va;
	va_start( va, format );
	vfprintf(outputStream, format, va);
}

void Log::error_log(const char* format, ...)
{
	if (!outputStream)
		return;
	print_timestamp("ERROR");
	va_list va;
	va_start( va, format );
	vfprintf(outputStream, format, va);
}

bool Log::EnableFileLogging(char* file)
{
	logfile = file;
	outputStream = fopen(logfile, "a");
#ifndef WIN32
	setlinebuf(outputStream);
#endif
	return true;
}

void Log::RotateLogFile(int sig)
{
	if (logfile != NULL)
	{	
		char savedLogFile[MAX_LOG_NAME_SIZE];
		fclose(outputStream);		// Does a flush internally
		time_t currentTime = time(0);
		if (strftime( savedLogFile, MAX_LOG_NAME_SIZE, "masterserver_%d%m%y%H%M%S.log", localtime(&currentTime) ) == 0)
			print_log("Error creating new log file");
		rename(logfile, savedLogFile);
		outputStream = fopen(logfile, "a");
#ifndef WIN32
		setlinebuf(outputStream);
#endif
	}
	else
	{
		print_log("Log file name not set, cannot rotate");
	}
}

long Log::GetLogSize()
{
	//int position = 0;
	//if (fgetpos(outputStream, (fpos_t*)&position) != 0)
	//	Log::error_log("Error reading log file size\n");
	return ftell(outputStream);
	//return position;
}
