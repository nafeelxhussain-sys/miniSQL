#pragma once
#include <iostream>
#include <string>
#include <stack>
#include "core.h"
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

class ConditionNode {
public:
    bool is_leaf;
    ConditionNode* left;
    ConditionNode* right;
    string operand;
    string column;
    string value;

    ConditionNode() : is_leaf(false), left(nullptr), right(nullptr) {}
};

class create_operation {
public:
    string table_name;
    string column_names[100];
    datatype column_dtypes[100];
    int column_size[100];
    int num_of_col;
    DB_error error;
};

class insert_operation {
public:
    string table_name;
    string column_data[100];
    int col_data_size;
    DB_error error;
};

class select_operation {
public:
    string table_name;
    ConditionNode* root;
    DB_error error;

    void print(ConditionNode* root);
};

string to_upper(string s);
int precedence(string op);

class operation {
public:
    string operation_type;
    DB_error error;

    select_operation select;
    insert_operation insert;
    create_operation create;
};

class where_clause {
public:
    DB_error make_tree(int token_count, int index, string tokens[], ConditionNode*& root);
};

class query_processor {
    #define max_tokens 100
    int token_count = 0;
    string tokens[max_tokens];

public:
    void print();
    void tokenizer(string input);

    operation command_router();

    create_operation parser_create();
    insert_operation parser_insert();
    select_operation parser_select();
};
