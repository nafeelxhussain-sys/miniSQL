#pragma once
#include <iostream>
using namespace std;

enum Error_type {
    ERR_NONE,
    ERR_SYNTAX,
    ERR_UNKNOWN_TABLE,
    ERR_UNKNOWN_COLUMN,
    ERR_TYPE_MISMATCH,
    ERR_RUNTIME
};

class DB_error {
public:
    Error_type type;
    string message;

    DB_error() : type(ERR_NONE), message("") {}
    DB_error(Error_type t, const string& msg) : type(t), message(msg) {}
    
    bool ok() const { return type == ERR_NONE; }
};

template <typename T>
T make_error(Error_type type, string msg) {
    T operation{};
    operation.error = DB_error(type, msg);
    return operation;
}