#include "executor.h"

// ------------------- execute -------------------
void executor::execute(operation &o, database &db) {
    if (!o.error.ok()) {
        cout << o.error.message << endl;
        return;
    }

    if (o.operation_type == "CREATE") execute_create(o.create, db);
    else if (o.operation_type == "INSERT") execute_insert(o.insert, db);
    else if (o.operation_type == "SELECT") execute_select(o.select, db);
}

// ------------------- CREATE -------------------
void executor::execute_create(create_operation &o, database &db) {
    if (!o.error.ok()) {
        cout << o.error.message << endl;
        return;
    }

    db.create_table(o.table_name, o.num_of_col, o.column_names, o.column_size, o.column_dtypes);
}

// ------------------- INSERT -------------------
void executor::execute_insert(insert_operation &o, database &db) {
    if (!o.error.ok()) {
        cout << o.error.message << endl;
        return;
    }

    db.insert_into_table(o.table_name, o.column_data, o.col_data_size);
}

// ------------------- SELECT -------------------
void executor::execute_select(select_operation &o, database &db) {
    if (!o.error.ok()) {
        cout << o.error.message << endl;
        return;
    }

    db.select_from_table(o.table_name);
}
