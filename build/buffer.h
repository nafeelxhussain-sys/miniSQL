#pragma once
#include<iostream>
#include<string>
#include "utils.h"
#include "error.h"
using namespace std;

class schema;
class ConditionNode;


class buffer {
public:
    int size;
    unsigned char* row_buffer;

    buffer();
    DB_error verify(schema& s, string* data, int size_of_data);
    void convert_and_write(const string& value, datatype dt, int position, int limit);
    void fill_buffer(schema& s, string* data, int size_of_data);
    void read_buffer(string path);
    void print_buffer(schema& s, ConditionNode *root,bool is_where);
};