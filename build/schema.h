#pragma once
#include<iostream>
#include"error.h"
#include"utils.h"
using namespace std;


class schema {
public:
    string table_name;
    uint8_t is_clustered;
    int row_size;
    int page_size;
    uint8_t num_of_cols;
    int* col_offset;
    int* col_index;
    string* column_name;
    datatype* dtypes;

    schema();
    ~schema();

    void free_arrays();
    DB_error load_schema(string db_name, string table_name);
    DB_error create_schema_file(string db_name, string table_name, uint8_t num_of_cols,
        string* name, int* size, datatype* type,int* col_index,uint8_t clustered=0);
    void print_schema();

    int getColumnIndex(string column_name);
    int getColumnOffset(int colIndex);
    int getColumnSize(int colIndex);
    datatype getColumnType(int colIndex);
    int getColumnIndexType(string column_name_);
};