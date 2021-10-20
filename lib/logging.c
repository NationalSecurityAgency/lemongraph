#include<stdarg.h>
#include<stdio.h>
#include<stdlib.h>
#include<time.h>
#include<unistd.h>

#include"logging.h"

const char *logstr(int level){
	static const char *levels[] = {
		"NOTSET",
		"DEBUG",
		"INFO",
		"WARNING",
		"ERROR",
		"CRITICAL",
	};
	static const int n = sizeof(levels) / sizeof(*levels);
	if(level < 0)
		abort();
	level = (level + 9) / 10;
	return level < n ? levels[level] : levels[n-1];
}

static void _logmsg(int level, const char *fmt, va_list ap){
	static char buf[512];
	static int ll = 0;
	static size_t nlen = 0;
	if(level > 0 && level < ll)
		return;
	size_t len, mlen, rlen;
	time_t now;
	now = time(NULL);
	len = strftime(buf, sizeof(buf), "%F %T %Z", localtime(&now));
	// overwrite null terminator
	buf[len++] = ' ';
	if(level < 0){
		ll = ~level;
		rlen = sizeof(buf) - len;
		nlen = vsnprintf(buf + len, rlen, fmt, ap);
		if(nlen >= rlen)
			abort();
		nlen += len;
		buf[nlen++] = '.';
		return;
	}
	len = nlen;
	rlen = sizeof(buf) - len;
	mlen = snprintf(buf + len, rlen, "%s: ", logstr(level));
	if(mlen >= rlen)
		abort();
	len += mlen;
	rlen -= mlen;
	mlen = vsnprintf(buf + len, rlen, fmt, ap);
	if(mlen >= rlen)
		abort();
	len += mlen;
	buf[len++] = '\n';
	(void)write(STDERR_FILENO, buf, len);
}

void vloginit(int level, const char *fmt, va_list ap){
	if(level < 0)
		abort();
	_logmsg(~level, fmt, ap);
}

void loginit(int level, const char *fmt, ...){
	va_list ap;
	va_start(ap, fmt);
	vloginit(level, fmt, ap);
	va_end(ap);
}

void vlogmsg(int level, const char *fmt, va_list ap){
	if(level < 0)
		abort();
	_logmsg(level, fmt, ap);
}

void logmsg(int level, const char *fmt, ...){
	va_list ap;
	va_start(ap, fmt);
	vlogmsg(level, fmt, ap);
	va_end(ap);
}
