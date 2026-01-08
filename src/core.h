#pragma once
#include <bits/stdc++.h>
using namespace std;

enum datatype{
    int32 = 0,
    text = 1,
    bool8 = 2
};

class schema {
public:
    string table_name;
    int row_size;
    int page_size;
    int num_of_cols;
    int* col_offset;
    string* column_name;
    datatype* dtypes;

    schema();
    ~schema();

    void free_arrays();
    void load_schema(string db_name, string table_name);
    void create_schema_file(string db_name, string table_name, int num_of_cols, string* name, int* size, datatype* type);
    void print_schema();

    int getColumnOffset(int colIndex);
    int getColumnSize(int colIndex);
    datatype getColumnType(int colIndex);
};

class buffer {
public:
    int size;
    unsigned char* row_buffer;

    buffer();
    bool verify(schema& s, string* data, int size_of_data);
    void convert_and_write(const string& value, datatype dt, int position, int limit);
    void fill_buffer(schema& s, string* data, int size_of_data);
    void read_buffer(string path);
    void print_buffer(schema& s);
};

class catalog {
    static const int MAX_TABLES = 50;

    string db_name;
    string table_names[MAX_TABLES];
    int coulumn_counts[MAX_TABLES];
    int table_count;

    void clear();

public:
    catalog();
    bool db_exists(string path);
    void save_catalog(string db_name);
    void load_catalog(string db_name);
    void add_table(string table_name, int coulumn_count);
    bool has_table(string table_name);
    void print_catalog();
};

class database {
public:
    string db_name;

    database(string name);
    bool table_exists(string tb_name);
    bool schema_exists(string tb_name);
    void create_table(string table_name, int num_of_cols, string* name, int* size, datatype* type);
    void insert_into_table(string table_name, string* data, int size_of_data);
    void select_from_table(string table_name);
};


