#include "mmap_base.h"
#include <iostream>
#include <string>

void Mmap_base::init()
{
	struct stat sb;

	if ((fd = open(fname, flags)) < 0)
	{
		std::string msg = "Could not open file ";
		msg += fname;
		throw Mmap_exception(msg);
	}

	if (fstat(fd, &sb) < 0)
	{
		std::string msg = "Could not stat file ";
		msg += fname;
		throw Mmap_exception(msg);
	}

	file_size = sb.st_size;
	data_pos = 0;
	map_block(data_pos);
	cur = data;
}

void Mmap_base::map_block(off_t pos)
{
	unmap();
	if (pos > file_size)
		throw Mmap_exception("Position exceeds file size");


	mapped_size = (pos + block_size > file_size) ? file_size - pos : block_size;

	if ((data = (char*)mmap(NULL, mapped_size , PROT_READ,
											MAP_PRIVATE, fd, pos)) == MAP_FAILED)
		throw Mmap_exception("mmap() failed");

	data_end = data + mapped_size;
}

void Mmap_base::unmap()
{
	if (data != MAP_FAILED)
	{
		if (munmap(data, mapped_size))
			throw Mmap_exception("munmap() failed");

		data = (char*)MAP_FAILED;
	}
}

void Mmap_base::end()
{
	unmap();
	close(fd);
	fd = -1;
}

char Mmap_base::read_next()
{
	char res = *cur++;

	if (cur == data_end && !is_eof())
	{
		data_pos += block_size;
		map_block(data_pos);
		cur = data;
	}

	return res;
}

bool Mmap_base::is_eof()
{
	return cur == data_end && data_pos + block_size >= file_size;
}

void Mmap_base::process_file()
{
	while (!is_eof())
	{
		handle_next(read_next());
	}
}

#ifdef TEST_MMAP_BASE
class Mmap_cat: public Mmap_base
{
	public:
		Mmap_cat(const char* fname, size_t block_size):
			Mmap_base(fname, block_size) {}

		void handle_next(char c);
};

void Mmap_cat::handle_next(char c)
{
	std::cout << c;
}

int main(int argc, char** argv)
{
	if (argc < 2)
	{
		std::cerr << "Missing file name" << std::endl;
		return -1;
	}

	size_t block_size = (1UL << 30); // 1 GB

	if (argc >= 3)
		block_size = std::stoull(argv[2]);

	Mmap_cat m(argv[1], block_size);

	m.init();
	m.process_file();
	return 0;
}
#endif
