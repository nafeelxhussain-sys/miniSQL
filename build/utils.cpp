#include <iostream>
#include <string>
#include "utils.h"
using namespace std;

// ------------------- utility -------------------

int precedence(string op) {
    if (to_upper(op) == "AND") return 2;
    if (to_upper(op) == "OR") return 1;
    return 0;
}


string to_upper(string s) {
    for (int i = 0; i < s.size(); i++)
        s[i] = toupper(s[i]);
    return s;
}

string dtype_to_string(datatype dt) {
    switch(dt) {
        case int32: return "INT";
        case text:  return "TEXT";
        case bool8: return "BOOL";
        default:    return "UNKNOWN";
    }
}