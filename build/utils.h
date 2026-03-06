#pragma once
#include <iostream>
#include <string>
using namespace std;

string to_upper(string s);
int precedence(string op);
enum datatype{
    int32 = 0,
    text = 1,
    bool8 = 2
};
struct primary_key{
    char pk[sizeof(int)];
};
string dtype_to_string(datatype dt);
int compare_keys(const void *buffer,datatype dt ,const void *key,int key_size);
int compare_composite(const void *buffer,datatype dt ,const void *key,int key_size);
string read_query();