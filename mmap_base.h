#ifndef MMAP_BASE_H
#define MMAP_BASE_H

#include <sys/mman.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>


#include <exception>
#include <string>

class Mmap_exception: public std::exception
{
protected:
	std::string msg;
	int sys_errno;
public:
	Mmap_exception(std::string msg):msg(msg)
	{
		sys_errno = errno;
		char buf[128];
		strerror_r(sys_errno, buf, sizeof(buf));
		msg += ": ";
		msg += buf;
	}

	const char* what() const noexcept
	{
		return msg.c_str();
	}
};

class Mmap_base
{
protected:
	const char* fname;
	off_t file_size;
	size_t block_size;
	int fd;
	char* data;
	off_t data_pos; // file offset data corresponds to
	char* data_end;
	char* cur; // current data pointer
	int flags;
	size_t mapped_size;

	void unmap();

public:
	Mmap_base(const char* fname, size_t block_size, bool read_only=true):fname(fname),block_size(block_size),
		fd(-1),data((char*)MAP_FAILED),data_pos(0),data_end(NULL),cur(NULL),mapped_size(0)
	{
		flags = read_only ? O_RDONLY : O_RDWR;
	}

	virtual ~Mmap_base()
	{
		end();
	}

	void init();
	void end();
	void map_block(off_t pos);
	char read_next();
	bool is_eof();
	void process_file();
	virtual void handle_next(char c) = 0;
};

#endif
