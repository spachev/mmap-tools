#ifndef IOBUF_H
#define IOBUF_H

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <exception>

class IO_buf
{
protected:
	const char* fname;
	size_t buf_size;
	char* buf;
	char* buf_end;
	char* cur_p;
	int fd;

public:
	IO_buf():
		buf(NULL),buf_end(NULL),cur_p(NULL),fd(-1) {}
	~IO_buf()
	{
		end();
	}

	void init(const char* fname, size_t buf_size = 4096);
	void end()
	{
		flush();
		close(fd);
		fd = -1;
		delete buf;
		buf_end = cur_p = buf = NULL;
	}

	void append(char c)
	{
		*cur_p++ = c;
		if (cur_p == buf_end)
			flush();
	}

	void append(const char* s, size_t len)
	{
		const char* s_end = s + len;
		for (;s < s_end;)
			append(*s++);
	}

	void flush()
	{
		size_t n_bytes = cur_p - buf;

		if (write(fd, buf, n_bytes) != n_bytes)
			throw std::exception(); // TODO: better exception

		cur_p = buf;
	}

};
#endif
