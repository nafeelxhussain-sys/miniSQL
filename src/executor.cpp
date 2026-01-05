#include<iostream>
#include<string>
#include"core.cpp"
#include"parser.cpp"
using namespace std;

class executor{
    public:
    
    void execute(operation &o , database &db){
        if(!o.error.ok()){
            cout<<o.error.message<<endl;
            return;
        }
        
        if(o.operation_type == "CREATE"){
            execute_create(o.create,db);
        }
        else if(o.operation_type == "INSERT"){
            execute_insert(o.insert,db);
        }
        else if(o.operation_type == "SELECT"){
            execute_select(o.select,db);
        }
    }
    
    void execute_create(create_operation &o , database &db){
        if(!o.error.ok()){
            cout<<o.error.message<<endl;
            return;
        }

        db.create_table(o.table_name,o.num_of_col,o.column_names,o.column_size,o.column_dtypes);
    }

    void execute_insert(insert_operation &o , database &db){
        if(!o.error.ok()){
            cout<<o.error.message<<endl;
            return;
        }

        db.insert_into_table(o.table_name,o.column_data,o.col_data_size);
    }

    void execute_select(select_operation &o , database &db){
        if(!o.error.ok()){
            cout<<o.error.message<<endl;
            return;
        }

        db.select_from_table(o.table_name);
    }
};