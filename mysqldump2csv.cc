#include "mysqldump2csv.h"
#include <sstream>
#include <iostream>

#include <getopt.h>

void MySQL_dump_parser::handle_next(char c)
{
	switch (state)
	{
		case INITIAL:
			handle_initial(c);
			break;
		case EXPECT_OPEN_PAREN:
			handle_expect_open_paren(c);
			break;
		case IN_VALUES:
			handle_in_values(c);
			break;
		case IN_QUOTE:
			handle_in_quote(c);
			break;
		case IN_ESC:
			handle_in_esc(c);
			break;
		case EXPECT_EOL:
			handle_expect_eol(c);
			break;
	}
}

void MySQL_dump_parser::handle_initial(char c)
{
	if (c == *line_match_p)
	{
		line_match_p++;

		if (line_match_p == line_match_end)
		{
			state = EXPECT_OPEN_PAREN;
		}

		return;
	}

	line_match_p = line_match_start;
}

void MySQL_dump_parser::handle_expect_open_paren(char c)
{
	if (c == '(')
		state = IN_VALUES;
}

void MySQL_dump_parser::handle_in_values(char c)
{
	switch (c)
	{
		case '\'':
			out_append(c);
			state = IN_QUOTE;
			break;
		case ',':
			process_val_buf();
			out_append(c);
			break;
		case ')':
			process_val_buf();
			state = EXPECT_EOL;
			break;
		default:
			val_buf_append(c); // val_buf is just to catch NULL, so we do not care to fill it in quotes
			break;
	}
}

void MySQL_dump_parser::handle_in_quote(char c)
{
	out_append(c);
	switch (c)
	{
		case '\'':
			state = IN_VALUES;
			break;
		case '\\':
			state = IN_ESC;
			break;
		default:
			break;
	}
}

void MySQL_dump_parser::handle_in_esc(char c)
{
	out_append(c);
	state = IN_QUOTE;
}

void MySQL_dump_parser::handle_expect_eol(char c)
{
	if (c == '\n')
	{
		out_append(c);
		state = INITIAL;
		next_out();
	}
}

void MySQL_dump_parser::out_append(char c)
{
	*cur_out << c;
}

void MySQL_dump_parser::out_append(const char* s)
{
	*cur_out << s;
}

void MySQL_dump_parser::next_out()
{
	cur_out++;
	if (cur_out == outs_end)
		cur_out = outs;
}


void MySQL_dump_parser::val_buf_append(char c)
{
	val_buf += c;
}

void MySQL_dump_parser::process_val_buf()
{
	if (val_buf == "NULL")
	{
		out_append("\\N");
	}
	else
		out_append(val_buf.c_str());

	val_buf = "";
}

void MySQL_dump_parser::open_outs()
{
	outs = new std::ofstream[n_out_files];

	for (size_t i = 0; i < n_out_files; i++)
	{
		std::stringstream fname;
		fname << output_base << (i+1) << ".csv";
		outs[i].open(fname.str());
		if (!outs[i])
		{
			std::string msg = "Could not open " + fname.str() + " for writing";
			throw Mmap_exception(msg);
		}
	}

	cur_out = outs;
	outs_end = outs + n_out_files;
}

void MySQL_dump_parser::close_outs()
{
	delete[] outs;
	outs = NULL;
}

static struct option long_options[] = {
				{"n-out-files",     required_argument, 0,  'n' },
				{"output-base",     required_argument, 0,  'o' },
				{"input-file",     required_argument, 0,  'i' },
				{"help", no_argument,       0,  'h' },
				{0,         0,                 0,  0 }
		};


void help()
{
	std::cout << "Valid options:" << std::endl;
	struct option* o = long_options;

	while (o->name)
	{
		std::cout << "--" << o->name << std::endl;
		o++;
	}
}

void die(const char* msg)
{
	std::cerr << msg << std::endl;
	exit(1);
}

int main(int argc, char** argv)
{
	size_t n_out_files = 4;
	const char* output_base = NULL;
	const char* input_file = NULL;
	size_t block_size = (1UL << 30);

	while (1)
	{
		int this_option_optind = optind ? optind : 1;
		int option_index = 0;

		int c = getopt_long(argc, argv, "n:o:i:h",
						long_options, &option_index);
		if (c == -1)
				break;

		switch (c)
		{
			case 'h':
				help();
				return 0;
			case 'n':
				n_out_files = atoll(optarg);
				break;
			case 'o':
				output_base = optarg;
				break;
			case 'i':
				input_file = optarg;
				break;
			default:
				help();
				return 1;
		}
	 }

	 if (!input_file)
		 die("Missing --input-file");

	 if (!output_base)
		 die("Missing --output-base");

	 try
	 {
		MySQL_dump_parser p(input_file, block_size, "REPLACE", output_base, n_out_files);
		 p.init();
		 p.process_file();
		 p.end();
	 }
	 catch (std::exception e)
	 {
		 std::cerr << "Error: " << e.what() << std::endl;
		 return 1;
	 }
}
