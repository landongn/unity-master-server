#include "Log.h"
#ifndef WIN32
#include <string.h>
#include <unistd.h>

bool WriteProcessID(char* processFullPath, char* pidFile, int bufSize)
{
	int pid = getpid();
	int lastSlash = -1;
	for (size_t i=0; i<strlen(processFullPath); ++i)
	{
		if (processFullPath[i] == '/')
			lastSlash = i;
	}
	if (lastSlash != -1)
	{
		snprintf(pidFile, bufSize, "%s.pid", processFullPath+lastSlash+1);
		Log::info_log("Writing PID to %s\n", pidFile);
		FILE* pidHandle = fopen(pidFile, "w");
		if (!pidHandle)
		{
			fprintf(stderr, "Failed to open %s\n", pidFile);
			return false;
		}
		if (!(fprintf(pidHandle, "%d", pid) > 0))
		{
			perror("Failed to write to pid file\n");
			return false;
		}
		fclose(pidHandle);
	}
	return true;
}
#endif
