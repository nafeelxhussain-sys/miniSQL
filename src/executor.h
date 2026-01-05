#ifndef EXECUTOR_H
#define EXECUTOR_H

#include <string>
#include "core.cpp"
#include "parser.cpp"

// Forward declarations of structs from parser
struct operation;
struct create_operation;
struct insert_operation;
struct select_operation;
class database;

class executor {
public:
    // Main dispatcher
    void execute(operation &o, database &db);

    // Specific operation handlers
    void execute_create(create_operation &o, database &db);
    void execute_insert(insert_operation &o, database &db);
    void execute_select(select_operation &o, database &db);
};

#endif
