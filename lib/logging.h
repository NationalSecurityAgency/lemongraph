#ifndef _LOGGING_H
#define _LOGGING_H

// chosen to match standard python log levels
#define LOG_NOTSET 0
#define LOG_DEBUG 10
#define LOG_INFO  20
#define LOG_WARN  30
#define LOG_ERROR 40
#define LOG_FATAL 50

#include<stdarg.h>

const char *logstr(int level);

void loginit(int level, const char *fmt, ...);
void vloginit(int level, const char *fmt, va_list ap);

void logmsg(int level, const char *fmt, ...);
void vlogmsg(int level, const char *fmt, va_list ap);

#endif
