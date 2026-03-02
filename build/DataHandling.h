#include<iostream>
#include"error.h"
#include"schema.h"
#include"filter.h"
using namespace std;

class DataHandler{
    public:
    DB_error set_verify(schema &s, SetClause &sc);
    DB_error data_verify(schema& s, string* data, int size_of_data) ;
    void print_row(schema& s, const char* row) ;
    void converter(schema& s, string *data, char* row) ;
    void converter(char* index_row, char* index_ptr, char* key_ptr, int index_size, int key_size) ;
};