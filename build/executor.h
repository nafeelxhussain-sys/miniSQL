#pragma once
#include <iostream>
#include <string>
#include "parser.h"
#include "core.h"
using namespace std;

class executor {
public:
    void execute(operation &o, database &db);
    void execute_create(create_operation &o, database &db);
    void execute_insert(insert_operation &o, database &db);
    void execute_delete(delete_operation &o, database &db);
    void execute_update(update_operation &o, database &db);
    void execute_select(select_operation &o, database &db);
    void execute_show(select_operation &o, database &db);
    void execute_drop(select_operation &o, database &db);
};
