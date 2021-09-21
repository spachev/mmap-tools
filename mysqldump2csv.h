#ifndef MYSQL_DUMP_PARSER_H
#define MYSQL_DUMP_PARSER_H

#include "mmap_base.h"
#include <fstream>

class MySQL_dump_parser: public Mmap_base
{
protected:
	enum {INITIAL, EXPECT_OPEN_PAREN, IN_VALUES, IN_QUOTE, IN_ESC, EXPECT_EOL} state;
	std::string val_buf;
	std::string line_match;
	const char* output_base;
	size_t n_out_files;
	const char* line_match_p;
	const char* line_match_end;
	const char* line_match_start;
	std::ofstream* outs;
	std::ofstream* cur_out;
	std::ofstream* outs_end;
	void handle_initial(char c);
	void handle_expect_open_paren(char c);
	void handle_in_values(char c);
	void handle_in_quote(char c);
	void handle_in_esc(char c);
	void handle_expect_eol(char c);
	void out_append(char c);
	void out_append(const char* s);
	void val_buf_append(char c);
	void process_val_buf();
	void open_outs();
	void close_outs();
	void next_out();

public:
	MySQL_dump_parser(const char* fname, size_t block_size, std::string line_match,
		const char* output_base, size_t n_out_files
	):Mmap_base(fname,block_size),
		state(INITIAL),line_match(line_match),output_base(output_base),n_out_files(n_out_files)
	{
		line_match_start = line_match_p = line_match.c_str();
		line_match_end = line_match_start + line_match.length();
		open_outs();
	}

	~MySQL_dump_parser()
	{
		close_outs();
	}

	void handle_next(char c);
};

#endif
