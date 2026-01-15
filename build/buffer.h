#pragma once
#include<iostream>
#include<string>
#include "utils.h"
#include "error.h"
using namespace std;

class schema;
class ConditionNode;
class SetClause;


class buffer {
public:
    int size;
    unsigned char* row_buffer;

    buffer();
    DB_error verify(schema& s, string* data, int size_of_data);
    DB_error set_verify(schema &s, SetClause &sc);
    void convert_and_write(const string& value, datatype dt, int position, int limit);
    void fill_buffer(schema& s, string* data, int size_of_data);
    void read_buffer(string path);
    void print_buffer(schema& s, ConditionNode *root,bool is_where);
    void update_buffer(schema& s, ConditionNode *root,bool is_where, SetClause& sc);
    int delete_from_buffer(schema& s, ConditionNode *root,bool is_where);
};