#pragma once
#include "error.h"
#include "filter.h"
#include "utils.h"
#include <bits/stdc++.h>
using namespace std;


class catalog {
    static const int MAX_TABLES = 50;

    string db_name;
    string table_names[MAX_TABLES];
    int coulumn_counts[MAX_TABLES];
    int table_count;

    void clear();

public:
    catalog();
    bool db_exists(string path);
    void save_catalog(string db_name);
    DB_error load_catalog(string db_name);
    DB_error add_table(string table_name, int coulumn_count);
    DB_error remove_table(string table_name, string db_name);
    bool has_table(string table_name);
    void print_catalog(string db_name);
};

class database {
public:
    string db_name;

    database(string name);
    bool table_exists(string tb_name);
    bool schema_exists(string tb_name);
    DB_error create_table(string table_name, int num_of_cols, string* name, int* size, datatype* type);
    DB_error insert_into_table(string table_name, string* data, int size_of_data);
    DB_error select_from_table(string table_name, ConditionNode* root, bool where);
    DB_error update_table(string table_name, ConditionNode* root, bool where,SetClause &sc);
    DB_error delete_from_table(string table_name, ConditionNode* root, bool where);
    DB_error drop_table(string table_name);
};


