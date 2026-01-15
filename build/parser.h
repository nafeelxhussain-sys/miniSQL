#pragma once
#include <iostream>
#include <string>
#include <stack>
#include "error.h"
#include "utils.h"
#include "filter.h"
using namespace std;



class create_operation {
public:
    #define max_columns 20
    string table_name;
    string column_names[max_columns];
    datatype column_dtypes[max_columns];
    int column_size[max_columns];
    int num_of_col;
    DB_error error;
};

class insert_operation {
public:
    #define max_ROWxCOLUMN 4096
    string table_name;
    string column_data[max_ROWxCOLUMN];
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
class update_operation {
public:
    string table_name;
    SetClause sc;
    ConditionNode* root;
    DB_error error;
};

class delete_operation {
public:
    string table_name;
    ConditionNode* root;
    DB_error error;
};

int precedence(string op);

class operation {
public:
    string operation_type;
    DB_error error;

    select_operation select;
    insert_operation insert;
    create_operation create;
    delete_operation delete_;
    update_operation update;
};


class query_processor {
    #define max_tokens 1024
    int token_count = 0;
    string tokens[max_tokens];

    public:
    void print();
    void tokenizer(string input);

    operation command_router();

    create_operation parser_create();
    insert_operation parser_insert();
    update_operation parser_update();
    delete_operation parser_delete();
    select_operation parser_select();
    select_operation parser_show();
    select_operation parser_drop();
};
