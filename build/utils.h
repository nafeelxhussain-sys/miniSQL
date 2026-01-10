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
string dtype_to_string(datatype dt);