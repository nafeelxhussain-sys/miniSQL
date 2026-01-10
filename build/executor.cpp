#include "executor.h"
#include "error.h"
#include "utils.h"
#include "schema.h"


// ------------------- execute -------------------
void executor::execute(operation &o, database &db) {
    if (!o.error.ok()) {
        cout << o.error.message << endl;
        return;
    }
    
    if (o.operation_type == "CREATE") execute_create(o.create, db);
    else if (o.operation_type == "INSERT") execute_insert(o.insert, db);
    else if (o.operation_type == "SELECT") execute_select(o.select, db);
    else if (o.operation_type == "SHOW") execute_show(o.select,db);
    else{
        cout<< "invalid syntax";
    }
    
}

// ------------------- CREATE -------------------
void executor::execute_create(create_operation &o, database &db) {
    if (!o.error.ok()) {
        cout << o.error.message << endl;
        return;
    }

    DB_error err = db.create_table(o.table_name, o.num_of_col, o.column_names, o.column_size, o.column_dtypes);

    if (!err.ok()) {
        cout << err.message << endl;
        return;
    }

    cout<<"table created : " + o.table_name<<endl;
}

// ------------------- INSERT -------------------
void executor::execute_insert(insert_operation &o, database &db) {
    if (!o.error.ok()) {
        cout << o.error.message << endl;
        return;
    }

    DB_error err =  db.insert_into_table(o.table_name, o.column_data, o.col_data_size);

    if (!err.ok()) {
        cout << err.message << endl;
        return;
    }

    cout<<"data inserted into : " + o.table_name<<endl;
}

// ------------------- SELECT -------------------
void executor::execute_select(select_operation &o, database &db) {
    if (!o.error.ok()) {
        cout << o.error.message << endl;
        return;
    }

    DB_error err;
    if(o.root == nullptr)
    err = db.select_from_table(o.table_name,o.root,false);
    else
    err = db.select_from_table(o.table_name,o.root,true);
    

    if (!err.ok()) {
        cout << err.message << endl;
        return;
    }
}
void executor::execute_show(select_operation &o, database &db) {
    if (!o.error.ok()) {
        cout << o.error.message << endl;
        return;
    }

    if(to_upper(o.table_name) == "DATABASE"){
        catalog c;
        DB_error err = c.load_catalog(db.db_name);

        if (!err.ok()) {
        cout << err.message << endl;
        return;
        }

        c.print_catalog(db.db_name);
    }
    else{
        schema s;
        DB_error err = s.load_schema(db.db_name,o.table_name);
        
        if (!err.ok()) {
            cout << err.message << endl;
            return;
        }
        
        s.print_schema();
    }
}
