#include "iobuf.h"

void IO_buf::init(const char* fname, size_t buf_size)
{
	this->fname = fname;
	this->buf_size = buf_size;
	if ((fd = open(fname, O_WRONLY|O_CREAT, 0644)) < 0)
		throw std::exception(); // TODO: better message and exception

	buf = new char[buf_size];
	buf_end = buf + buf_size;
	cur_p = buf;
}
