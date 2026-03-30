#include <cstdio>
#include "executor.h"
#include "QueryOptimiser.h"
#include "BplusTree.h"
#include "Index.h"
#include "DiskManager.h"
#include "DataHandling.h"
#include "error.h"
#include "utils.h"
#include "error.h"
#include "schema.h"


// ------------------- execute -------------------
void executor::execute(operation &o, AccessPath &acc_path) {
    if (!o.error.ok()) {
        cout << o.error.message << endl;
        return;
    }
    
    if (o.operation_type == "CREATE") {
        if(acc_path.type!=heap) execute_create_cluster(o.create);
        else  execute_create_heap(o.create);
    }

    else if (o.operation_type == "INSERT") {
        if(acc_path.type!=heap) execute_insert_cluster(o.insert);
        else execute_insert_heap(o.insert);
    }

    else if (o.operation_type == "SELECT") {
        if(acc_path.type==1) execute_select_pklookup(o.select, acc_path);
        else if(acc_path.type==2) execute_select_sklookup(o.select, acc_path);
        else if(acc_path.type==3) execute_select_pkrange(o.select, acc_path);
        else if(acc_path.type==4) execute_select_skrange(o.select, acc_path);
        else if(acc_path.type==5) execute_select_fullscan(o.select, acc_path);
        else if(acc_path.type==7) execute_select_heap(o.select);
    }

    else if (o.operation_type == "UPDATE") {
        if(acc_path.type==1) execute_update_pklookup(o.update, acc_path);
        else if(acc_path.type==2) execute_update_sklookup(o.update, acc_path);
        else if(acc_path.type==3) execute_update_pkrange(o.update, acc_path);
        else if(acc_path.type==4) execute_update_skrange(o.update, acc_path);
        else if(acc_path.type==5) execute_update_fullscan(o.update, acc_path);
        else if(acc_path.type==7) execute_update_heap(o.update);
    }

    else if (o.operation_type == "DELETE") {
        if(acc_path.type==1) execute_delete_pklookup(o.delete_, acc_path);
        else if(acc_path.type==2) execute_delete_sklookup(o.delete_, acc_path);
        else if(acc_path.type==3) execute_delete_pkrange(o.delete_, acc_path);
        else if(acc_path.type==4) execute_delete_skrange(o.delete_, acc_path);
        else if(acc_path.type==5) execute_delete_fullscan(o.delete_, acc_path);
        else if(acc_path.type==7) execute_delete_heap(o.delete_);
    }

    else if (o.operation_type == "DROP") {
        if(acc_path.type!=heap)execute_drop_cluster(o.select, acc_path);
        else execute_drop_heap(o.select);
    }


    else if (o.operation_type == "SHOW") execute_show(o.select);

    else{
        cout<< "invalid syntax : ";
    }
    
}

// ------------------- CREATE -------------------
void executor::execute_create_heap(create_operation &o) {
    if (!o.error.ok()) {
        cout << o.error.message << endl;
        return;
    }

    database db("main");
    DB_error err = db.create_table(o.table_name, o.num_of_col, o.column_names, o.column_size, o.column_dtypes,o.column_index);

    if (!err.ok()) {
        cout << err.message << endl;
        return;
    }

    cout<<"table created : " + o.table_name<<endl;
}

// ------------------- INSERT -------------------
void executor::execute_insert_heap(insert_operation &o) {
    if (!o.error.ok()) {
        cout << o.error.message << endl;
        return;
    }

    database db("main");
    DB_error err =  db.insert_into_table(o.table_name, o.column_data, o.col_data_size);

    if (!err.ok()) {
        cout << err.message << endl;
        return;
    }

    cout<<"data inserted into : " + o.table_name<<endl;
}

// ------------------- SELECT -------------------
void executor::execute_select_heap(select_operation &o) {
    if (!o.error.ok()) {
        cout << o.error.message << endl;
        return;
    }

    database db("main");
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
void executor::execute_show(select_operation &o) {
    if (!o.error.ok()) {
        cout << o.error.message << endl;
        return;
    }

    database db("main");

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

void executor :: execute_delete_heap(delete_operation &o){
    if (!o.error.ok()) {
        cout << o.error.message << endl;
        return;
    }

    database db("main");

    DB_error err ;
    if(o.root == nullptr)
    err = db.delete_from_table(o.table_name , o.root , false);
    else
    err = db.delete_from_table(o.table_name , o.root , true);

    if (!err.ok()) {
        cout << err.message << endl;
        return;
    }

    cout<<"data deleted from : " + o.table_name<<endl;
}

void executor :: execute_update_heap(update_operation &o){
    if (!o.error.ok()) {
        cout << o.error.message << endl;
        return;
    }

    database db("main");

    DB_error err ;
    if(o.root == nullptr)
    err = db.update_table(o.table_name , o.root , false,o.sc);
    else
    err = db.update_table(o.table_name , o.root , true,o.sc);

    if (!err.ok()) {
        cout << err.message << endl;
        return;
    }

    cout<<"data updated into : " + o.table_name<<endl;
}

void executor::execute_drop_heap(select_operation &o) {
    if (!o.error.ok()) {
        cout << o.error.message << endl;
        return;
    }

    database db("main");
    DB_error err = db.drop_table(o.table_name);

    if (!err.ok()) {
        cout << err.message << endl;
        return;
    }

    cout<<"table deleted : " + o.table_name<<endl;
}


void executor::execute_create_cluster(create_operation &o){
    if (!o.error.ok()) {
        cout << o.error.message << endl;
        return;
    }

    // handle catalog
    catalog cat;
    if (!cat.db_exists("main.catalog")) {
        cat.save_catalog("main");
    }
    o.error = cat.load_catalog("main");
    if(!o.error.ok()){
        cout<<o.error.message<<endl;
        return;
    }
    
    if(cat.has_table(o.table_name)){
        o.error = DB_error(ERR_RUNTIME,"table " + o.table_name + " already exists");
        cout<<o.error.message<<endl;
        return;
    }
    cat.add_table(o.table_name,o.num_of_col,1);

    //handle schema
    schema s;
    o.error = s.create_schema_file("main", o.table_name, o.num_of_col, o.column_names,o.column_size, o.column_dtypes, o.column_index,1);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }
    o.error = s.load_schema("main",o.table_name);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }
    

    Disk_Manager dm;
    Table_Metadata tmd;
    datatype key_dt;
    int key_size;
    int row_size=s.row_size;
    bool primary=true;

    //find pk column
    for(int col = 0 ; col<o.num_of_col ; col++){
        if(o.column_index[col]==1){
            key_dt = o.column_dtypes[col];
            key_size = o.column_size[col];
            break;
        }
    }


    dm.create_file(o.table_name);

    //create tree file
    BplusTree tree(dm,tmd,key_dt,key_size,primary);
    tree.create_tree(row_size);

    //create index files
    for(int col = 0 ; col<o.num_of_col ; col++){
        if(o.column_index[col]==2){
            Disk_Manager dm_idx;
            Table_Metadata tmd_idx;
            datatype idx_dt = o.column_dtypes[col];
            int index_size = o.column_size[col];
            

            dm_idx.create_index(o.table_name, o.column_names[col]);
            Index idx(dm_idx,tmd_idx,idx_dt,key_size,index_size);
            idx.create_index();
        }
    }


    cout<<"table created : " + o.table_name<<endl;
}

void executor::execute_insert_cluster(insert_operation &o){
    string file_name = "..\\data\\main_"  + o.table_name + ".tbl";

    // handle catalog
    catalog cat;
    if (!cat.db_exists("..\\data\\main.catalog")) {
        o.error = DB_error(ERR_UNKNOWN_TABLE, "catalog not found: ");
        cout<<o.error.message<<endl;
        return;
    }
    o.error = cat.load_catalog("main");
    if(!o.error.ok()){
        cout<<o.error.message<<endl;
        return;
    }
    if(!cat.has_table(o.table_name)){
        o.error = DB_error(ERR_RUNTIME,"table " + o.table_name + " does not exists");
        cout<<o.error.message<<endl;
        return;
    }
 
    //handle schema
    schema s;
    o.error = s.load_schema("main",o.table_name);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //data verification
    DataHandler dh;
    o.error = dh.data_verify(s,o.column_data,o.col_data_size);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //load the main file
    Disk_Manager dm_main;
    Table_Metadata tmd_main;
    
    dm_main.open_file(o.table_name);
    BplusTree tree(dm_main, tmd_main,int32, sizeof(int),true);
    tree.open_tree();
    
    
    //load the index files
    int cnt = s.getIndexCount();
    
    Disk_Manager dm_index[cnt];
    Table_Metadata tmd_index[cnt];
    Index* tree_index[cnt];
    int reference[s.num_of_cols];//colIndex-->treeIndex
 
    for (int i = 0; i < s.num_of_cols; i++)
    reference[i] = -1;
    
    for(int i = 0, j = 0; i < s.num_of_cols ; i++){
        if(s.getColumnIndexType(i)==2){
            dm_index[j].open_index(o.table_name, s.column_name[i]);
            tree_index[j] = new Index(dm_index[j],tmd_index[j],s.getColumnType(i),sizeof(int),s.getColumnSize(i));
            tree_index[j]->open_index();
            reference[i]=j;
            j++;
        }
    }
    
    int rows_to_insert = o.col_data_size / s.num_of_cols;
    int key_off = s.getPkOffset();
    bool oper = true;

    try{
    for(int r = 0 ; r<rows_to_insert ; r++){
        //insert into main file
        char row_main[s.row_size];
        string* col_ptr = o.column_data + r*s.num_of_cols ;

        dh.converter(s,col_ptr,row_main);
        tree.insert_row((void*)row_main,s.row_size,key_off);

        for(int i = 0; i<s.num_of_cols ; i++){
            //insert into index files
            int j = reference[i];
            if(j == -1) continue;

            int index_size = s.getColumnSize(i);
            int index_row_size = index_size + sizeof(int);
            char* index_ptr = row_main + s.getColumnOffset(i);
            char* key_ptr = row_main + key_off;
            char row_index[index_row_size];

            dh.converter(row_index,index_ptr,key_ptr,index_size,sizeof(int));
            tree_index[j]->insert_index(row_index);
        }
    }}
    catch(DB_error &e){
        cout<<e.message<<endl;
        oper = false;
    }

    for (int i = 0; i < cnt; i++) {
        delete tree_index[i];
    }
    
    if(oper)
    cout<<"data inserted into : " + o.table_name<<endl;
    return;
}

void executor::execute_select_pklookup(select_operation &o, AccessPath &acc){
    string file_name = "..\\data\\main_"  + o.table_name + ".tbl";

    // handle catalog
    catalog cat;
    if (!cat.db_exists("..\\data\\main.catalog")) {
        o.error = DB_error(ERR_UNKNOWN_TABLE, "catalog not found: ");
        cout<<o.error.message<<endl;
        return;
    }
    o.error = cat.load_catalog("main");
    if(!o.error.ok()){
        cout<<o.error.message<<endl;
        return;
    }
    if(!cat.has_table(o.table_name)){
        o.error = DB_error(ERR_RUNTIME,"table " + o.table_name + " does not exists");
        cout<<o.error.message<<endl;
        return;
    }
 
    //handle schema
    schema s;
    o.error = s.load_schema("main",o.table_name);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //load the main file
    Disk_Manager dm_main;
    Table_Metadata tmd_main;
    
    dm_main.open_file(o.table_name);
    BplusTree tree(dm_main, tmd_main,int32, sizeof(int),true);
    tree.open_tree();

    //read the main file
    DataHandler dh;
    where_clause wh;
    bool found = false;
    int row_size = s.row_size;
    int key_off = s.getPkOffset();
    char search_key[sizeof(int)];
    dh.converter(search_key, acc.search_value, int32, sizeof(int));

    try{
    tree.scan_point(row_size,search_key, key_off,[&](const char* ptr){
        if(wh.evaluvate_tree(o.root, s, (const unsigned char*)ptr)){
        dh.print_row(s,ptr);
        found = true;
        }
    });}
    catch(DB_error &db){
        cout<<db.message<<endl;
        return;
    }

    if(!found){
        cout<<"No matches found: "<<endl;
    }
}

void executor::execute_select_sklookup(select_operation &o, AccessPath &acc){
    string file_name = "..\\data\\main_"  + o.table_name + ".tbl";

    // handle catalog
    catalog cat;
    if (!cat.db_exists("..\\data\\main.catalog")) {
        o.error = DB_error(ERR_UNKNOWN_TABLE, "catalog not found: ");
        cout<<o.error.message<<endl;
        return;
    }
    o.error = cat.load_catalog("main");
    if(!o.error.ok()){
        cout<<o.error.message<<endl;
        return;
    }
    if(!cat.has_table(o.table_name)){
        o.error = DB_error(ERR_RUNTIME,"table " + o.table_name + " does not exists");
        cout<<o.error.message<<endl;
        return;
    }
 
    //handle schema
    schema s;
    o.error = s.load_schema("main",o.table_name);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //load the main file
    Disk_Manager dm_main;
    Table_Metadata tmd_main;
    
    dm_main.open_file(o.table_name);
    BplusTree tree(dm_main, tmd_main,int32, sizeof(int),true);
    tree.open_tree();
    
    
    //load the index file    
    Disk_Manager dm_index;
    Table_Metadata tmd_index;
    
    int col_no = s.getColumnIndex(acc.col_name);
    datatype dt = s.getColumnType(col_no);
    int index_size = s.getColumnSize(col_no);

    dm_index.open_index(o.table_name , acc.col_name);
    Index tree_index(dm_index, tmd_index, dt, sizeof(int), index_size);
    tree_index.open_index();
    

    //read the index file
    DataHandler dh;
    where_clause wh;
    bool found = false;
    int index_row_size = index_size + sizeof(int);
    int primary_row_size = s.row_size;
    int key_off = s.getPkOffset();
    char search_key[index_row_size];
    dh.converter(search_key, acc.search_value, dt, index_row_size);

    try{
    tree_index.find_all_pks(search_key,dt,[&](const char* ptr_to_index){
        tree.scan_point(primary_row_size, ptr_to_index,key_off, [&](const char* ptr_to_prim){
            if(wh.evaluvate_tree(o.root, s, (const unsigned char*)ptr_to_prim)){
            dh.print_row(s,ptr_to_prim);
            found = true;
        }});
    });}
    catch(DB_error &db){
        cout<<db.message<<endl;
        return;
    }

    if(!found){
        cout<<"No matches found: "<<endl;
    }
}

void executor::execute_select_pkrange(select_operation &o, AccessPath &acc){
    string file_name = "..\\data\\main_"  + o.table_name + ".tbl";

    // handle catalog
    catalog cat;
    if (!cat.db_exists("..\\data\\main.catalog")) {
        o.error = DB_error(ERR_UNKNOWN_TABLE, "catalog not found: ");
        cout<<o.error.message<<endl;
        return;
    }
    o.error = cat.load_catalog("main");
    if(!o.error.ok()){
        cout<<o.error.message<<endl;
        return;
    }
    if(!cat.has_table(o.table_name)){
        o.error = DB_error(ERR_RUNTIME,"table " + o.table_name + " does not exists");
        cout<<o.error.message<<endl;
        return;
    }
 
    //handle schema
    schema s;
    o.error = s.load_schema("main",o.table_name);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //load the main file
    Disk_Manager dm_main;
    Table_Metadata tmd_main;
    
    dm_main.open_file(o.table_name);
    BplusTree tree(dm_main, tmd_main,int32, sizeof(int),true);
    tree.open_tree();

    //read the main file
    DataHandler dh;
    where_clause wh;
    bool found = false;
    int row_size = s.row_size;
    int key_off = s.getPkOffset();
    char search_key[sizeof(int)];
    dh.converter(search_key, acc.search_value, int32, sizeof(int));

    try{
    if(acc.forward){
        tree.scan_forward(row_size,search_key, key_off,[&](const char* ptr){
        if(wh.evaluvate_tree(o.root, s, (const unsigned char*)ptr)){
            dh.print_row(s,ptr);
            found = true;
        }});
    }
    else{
        tree.scan_backward(row_size,search_key, key_off,[&](const char* ptr){
        if(wh.evaluvate_tree(o.root, s, (const unsigned char*)ptr)){
            dh.print_row(s,ptr);
            found = true;
        }});
    }}
    catch(DB_error &db){
        cout<<db.message<<endl;
        return;
    }

    if(!found){
        cout<<"No matches found: "<<endl;
    }
}

void executor::execute_select_skrange(select_operation &o, AccessPath &acc){
    string file_name = "..\\data\\main_"  + o.table_name + ".tbl";

    // handle catalog
    catalog cat;
    if (!cat.db_exists("..\\data\\main.catalog")) {
        o.error = DB_error(ERR_UNKNOWN_TABLE, "catalog not found: ");
        cout<<o.error.message<<endl;
        return;
    }
    o.error = cat.load_catalog("main");
    if(!o.error.ok()){
        cout<<o.error.message<<endl;
        return;
    }
    if(!cat.has_table(o.table_name)){
        o.error = DB_error(ERR_RUNTIME,"table " + o.table_name + " does not exists");
        cout<<o.error.message<<endl;
        return;
    }
 
    //handle schema
    schema s;
    o.error = s.load_schema("main",o.table_name);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //load the main file
    Disk_Manager dm_main;
    Table_Metadata tmd_main;
    
    dm_main.open_file(o.table_name);
    BplusTree tree(dm_main, tmd_main,int32, sizeof(int),true);
    tree.open_tree();
    
    
    //load the index file    
    Disk_Manager dm_index;
    Table_Metadata tmd_index;
    
    int col_no = s.getColumnIndex(acc.col_name);
    datatype dt = s.getColumnType(col_no);
    int index_size = s.getColumnSize(col_no);

    dm_index.open_index(o.table_name , acc.col_name);
    Index tree_index(dm_index, tmd_index, dt, sizeof(int), index_size);
    tree_index.open_index();
    

    //read the index file
    DataHandler dh;
    where_clause wh;
    bool found = false;
    int index_row_size = index_size + sizeof(int);
    int primary_row_size = s.row_size;
    int key_off = s.getPkOffset();
    char search_key[index_row_size];
    dh.converter(search_key, acc.search_value, dt, index_row_size);

    try{
    if(acc.forward){
        tree_index.find_all_pks_forward(search_key,dt,[&](const char* ptr_to_index){
            tree.scan_point(primary_row_size, ptr_to_index,key_off, [&](const char* ptr_to_prim){
                if(wh.evaluvate_tree(o.root, s, (const unsigned char*)ptr_to_prim)){
                    dh.print_row(s,ptr_to_prim);
                    found = true;
                }
            });
        });
        }
    else{
        tree_index.find_all_pks_backward(search_key,dt,[&](const char* ptr_to_index){
            tree.scan_point(primary_row_size, ptr_to_index,key_off, [&](const char* ptr_to_prim){
                if(wh.evaluvate_tree(o.root, s, (const unsigned char*)ptr_to_prim)){
                dh.print_row(s,ptr_to_prim);
                found = true;
                }
            });
        });
    }}
    catch(DB_error &db){
        cout<<db.message<<endl;
        return;
    }

    if(!found){
        cout<<"No matches found: "<<endl;
    }
}

void executor::execute_select_fullscan(select_operation &o, AccessPath &acc){
    string file_name = "..\\data\\main_"  + o.table_name + ".tbl";

    // handle catalog
    catalog cat;
    if (!cat.db_exists("..\\data\\main.catalog")) {
        o.error = DB_error(ERR_UNKNOWN_TABLE, "catalog not found: ");
        cout<<o.error.message<<endl;
        return;
    }
    o.error = cat.load_catalog("main");
    if(!o.error.ok()){
        cout<<o.error.message<<endl;
        return;
    }
    if(!cat.has_table(o.table_name)){
        o.error = DB_error(ERR_RUNTIME,"table " + o.table_name + " does not exists");
        cout<<o.error.message<<endl;
        return;
    }
 
    //handle schema
    schema s;
    o.error = s.load_schema("main",o.table_name);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //load the main file
    Disk_Manager dm_main;
    Table_Metadata tmd_main;
    
    dm_main.open_file(o.table_name);
    BplusTree tree(dm_main, tmd_main,int32, sizeof(int),true);
    tree.open_tree();

    //read the main file
    DataHandler dh;
    where_clause wh;
    bool found = false;
    int row_size = s.row_size;

    try{
    tree.scan_all(row_size,[&](const char* ptr){
    if(wh.evaluvate_tree(o.root, s, (const unsigned char*)ptr)){
        dh.print_row(s,ptr);
        found = true;
    }});
    }
    catch(DB_error &db){
        cout<<db.message<<endl;
        return;
    }
    

    if(!found){
        cout<<"No matches found: "<<endl;
    }
}

void executor::execute_update_pklookup(update_operation &o, AccessPath &acc){
    string file_name = "..\\data\\main_"  + o.table_name + ".tbl";

    // handle catalog
    catalog cat;
    if (!cat.db_exists("..\\data\\main.catalog")) {
        o.error = DB_error(ERR_UNKNOWN_TABLE, "catalog not found: ");
        cout<<o.error.message<<endl;
        return;
    }
    o.error = cat.load_catalog("main");
    if(!o.error.ok()){
        cout<<o.error.message<<endl;
        return;
    }
    if(!cat.has_table(o.table_name)){
        o.error = DB_error(ERR_RUNTIME,"table " + o.table_name + " does not exists");
        cout<<o.error.message<<endl;
        return;
    }
 
    //handle schema
    schema s;
    o.error = s.load_schema("main",o.table_name);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //set verification
    DataHandler dh;
    o.error = dh.set_verify(s,o.sc);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //load the main file
    Disk_Manager dm_main;
    Table_Metadata tmd_main;
    
    dm_main.open_file(o.table_name);
    BplusTree tree(dm_main, tmd_main,int32, sizeof(int),true);
    tree.open_tree();
    
    
    //load the index files
    int cnt = o.sc.set_cols;
    
    Disk_Manager dm_index[cnt];
    Table_Metadata tmd_index[cnt];
    Index* tree_index[cnt] ={nullptr};
    int reference[s.num_of_cols];//colIndex-->treeIndex
    
    for (int i = 0; i < s.num_of_cols; i++)
    reference[i] = -1;
    
    for(int i = 0, j = 0; i < o.sc.set_cols ; i++){
        string col_name = o.sc.column_names[i];
        int col_no = s.getColumnIndex(col_name);
        
        if(s.getColumnIndexType(col_name)==2){
            dm_index[j].open_index(o.table_name, col_name);
            tree_index[j] = new Index(dm_index[j],tmd_index[j],s.getColumnType(col_no),sizeof(int),s.getColumnSize(col_no));
            tree_index[j]->open_index();
            reference[s.getColumnIndex(col_name)]=j;
            j++;
        }
    }
    
    int key_off = s.getPkOffset();
    int main_row_size = s.row_size;
    bool oper = true;

    //updating.....
    try{
        char key_main[sizeof(int)];
        dh.converter(key_main, acc.search_value,int32,sizeof(int));
        where_clause wh;

        tree.scan_point(main_row_size, key_main, key_off,[&](const char* ptr_to_main){
            if(wh.evaluvate_tree(o.root, s, (unsigned char*)ptr_to_main)){
            for(int i = 0 ; i < cnt ; i++){
                //update into index files
                int col_no = s.getColumnIndex(o.sc.column_names[i]);

                if(reference[col_no] == -1){
                //update into main file
                int index_size = s.getColumnSize(col_no);
                int index_off = s.getColumnOffset(col_no);
                char new_index_key[index_size];
                dh.converter(new_index_key, o.sc.column_values[i], s.getColumnType(col_no), index_size);
                tree.update_row(key_main,main_row_size,key_off,index_off,index_size,new_index_key);
                }
                else{
                int index_size = s.getColumnSize(col_no);
                int index_row_size = index_size + sizeof(int);
                int index_off = s.getColumnOffset(col_no);

                char old_row_index[index_row_size];
                char new_row_index[index_row_size];
                char new_index_key[index_size];
                char* old_index_ptr = (char*)ptr_to_main  + index_off;

                dh.converter(new_index_key, o.sc.column_values[i], s.getColumnType(col_no), index_size);

                dh.converter(old_row_index, old_index_ptr, key_main , index_size, sizeof(int));
                dh.converter(new_row_index, new_index_key, key_main , index_size, sizeof(int));

                tree_index[reference[col_no]]->update_index(old_row_index, new_row_index);

                //update into main file
                tree.update_row(key_main,main_row_size,key_off,index_off,index_size,new_index_key);
                }
            }}
        });    
    }
    catch(DB_error &e){
        cout<<e.message<<endl;
        oper = false;
    }

    for (int i = 0; i < cnt; i++) {
        delete tree_index[i];
    }
    
    if(oper)
    cout<<"data updated into : " + o.table_name<<endl;
    return;
}

void executor::execute_update_sklookup(update_operation &o, AccessPath &acc){
    string file_name = "..\\data\\main_"  + o.table_name + ".tbl";

    // handle catalog
    catalog cat;
    if (!cat.db_exists("..\\data\\main.catalog")) {
        o.error = DB_error(ERR_UNKNOWN_TABLE, "catalog not found: ");
        cout<<o.error.message<<endl;
        return;
    }
    o.error = cat.load_catalog("main");
    if(!o.error.ok()){
        cout<<o.error.message<<endl;
        return;
    }
    if(!cat.has_table(o.table_name)){
        o.error = DB_error(ERR_RUNTIME,"table " + o.table_name + " does not exists");
        cout<<o.error.message<<endl;
        return;
    }
 
    //handle schema
    schema s;
    o.error = s.load_schema("main",o.table_name);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //set verification
    DataHandler dh;
    o.error = dh.set_verify(s,o.sc);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //load the main file
    Disk_Manager dm_main;
    Table_Metadata tmd_main;
    
    dm_main.open_file(o.table_name);
    BplusTree tree(dm_main, tmd_main,int32, sizeof(int),true);
    tree.open_tree();
    
    
    //load the index files
    int cnt = o.sc.set_cols;
    
    Disk_Manager dm_index[cnt];
    Table_Metadata tmd_index[cnt];
    Index* tree_index[cnt] ={nullptr};
    int reference[s.num_of_cols];//colIndex-->treeIndex
    
    for (int i = 0; i < s.num_of_cols; i++)
    reference[i] = -1;
    
    for(int i = 0, j = 0; i < o.sc.set_cols ; i++){
        string col_name = o.sc.column_names[i];
        int col_no = s.getColumnIndex(col_name);
        
        if(s.getColumnIndexType(col_name)==2){
            dm_index[j].open_index(o.table_name, col_name);
            tree_index[j] = new Index(dm_index[j],tmd_index[j],s.getColumnType(col_no),sizeof(int),s.getColumnSize(col_no));
            tree_index[j]->open_index();
            reference[s.getColumnIndex(col_name)]=j;
            j++;
        }
    }

    //load the index file which is to scan
    Disk_Manager dm_index_scan;
    Table_Metadata tmd_index_scan;
    Index* tree_index_scan;

    int col_no = s.getColumnIndex(acc.col_name);
    int ref = reference[col_no];
    if(ref == -1){
        dm_index_scan.open_index(o.table_name, acc.col_name);
        tree_index_scan = new Index(dm_index_scan,tmd_index_scan,s.getColumnType(col_no),sizeof(int),s.getColumnSize(col_no));
        tree_index_scan->open_index();
    }
    else{
        tree_index_scan = tree_index[ref];
    }
    
    where_clause wh;
    datatype dt_index_scan = s.getColumnType(col_no);
    int index_scan_size = s.getColumnSize(col_no);
    int key_off = s.getPkOffset();
    int main_row_size = s.row_size;
    char key_scan[index_scan_size];
    dh.converter(key_scan, acc.search_value,dt_index_scan,index_scan_size);
    bool oper = true;
    cout<<dt_index_scan<<endl;
    string str(key_scan, index_scan_size);
    cout << str<<endl;

    vector<primary_key> pks;
    
    //updating.....
    try{
        tree_index_scan->find_all_pks(key_scan,dt_index_scan,[&](const char* ptr_to_index_scan){
            primary_key pk_store;
            memcpy(&pk_store.pk, ptr_to_index_scan, sizeof(int));
            pks.push_back(pk_store);  
        });

        for(int i = 0 ; i<pks.size() ; i++){
        char* key_main = pks[i].pk;
        tree.scan_point(main_row_size,key_main,key_off,[&](const char* ptr_to_main){
            if(wh.evaluvate_tree(o.root, s, (unsigned char*)ptr_to_main)){
            for(int i = 0 ; i < cnt ; i++){
            int col_no = s.getColumnIndex(o.sc.column_names[i]);

            if(reference[col_no] == -1){
            //update into main file
            int index_size = s.getColumnSize(col_no);
            int index_off = s.getColumnOffset(col_no);
            char new_index_key[index_size];
            dh.converter(new_index_key, o.sc.column_values[i], s.getColumnType(col_no), index_size);
            tree.update_row(key_main,main_row_size,key_off,index_off,index_size,new_index_key);
            }
            else{
            //updateinto index fils
            int index_size = s.getColumnSize(col_no);
            int index_row_size = index_size + sizeof(int);
            int index_off = s.getColumnOffset(col_no);

            char old_row_index[index_row_size];
            char new_row_index[index_row_size];
            char new_index_key[index_size];
            char* old_index_ptr = (char*)ptr_to_main  + index_off;

            dh.converter(new_index_key, o.sc.column_values[i], s.getColumnType(col_no), index_size);

            dh.converter(old_row_index, old_index_ptr, key_main , index_size, sizeof(int));
            dh.converter(new_row_index, new_index_key, key_main , index_size, sizeof(int));

            tree_index[reference[col_no]]->update_index(old_row_index, new_row_index);

            //update into main file
            tree.update_row(key_main,main_row_size,key_off,index_off,index_size,new_index_key);
            }
        }}});}
    }
    catch(DB_error &e){
        cout<<e.message<<endl;
        oper = false;
    }

    for (int i = 0; i < cnt; i++) {
        delete tree_index[i];
    }
    
    if(oper)
    cout<<"data updated into : " + o.table_name<<endl;
    return;
}

void executor::execute_update_pkrange(update_operation &o, AccessPath &acc){
    string file_name = "..\\data\\main_"  + o.table_name + ".tbl";

    // handle catalog
    catalog cat;
    if (!cat.db_exists("..\\data\\main.catalog")) {
        o.error = DB_error(ERR_UNKNOWN_TABLE, "catalog not found: ");
        cout<<o.error.message<<endl;
        return;
    }
    o.error = cat.load_catalog("main");
    if(!o.error.ok()){
        cout<<o.error.message<<endl;
        return;
    }
    if(!cat.has_table(o.table_name)){
        o.error = DB_error(ERR_RUNTIME,"table " + o.table_name + " does not exists");
        cout<<o.error.message<<endl;
        return;
    }
 
    //handle schema
    schema s;
    o.error = s.load_schema("main",o.table_name);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //set verification
    DataHandler dh;
    o.error = dh.set_verify(s,o.sc);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //load the main file
    Disk_Manager dm_main;
    Table_Metadata tmd_main;
    
    dm_main.open_file(o.table_name);
    BplusTree tree(dm_main, tmd_main,int32, sizeof(int),true);
    tree.open_tree();
    
    
    //load the index files
    int cnt = o.sc.set_cols;
    
    Disk_Manager dm_index[cnt];
    Table_Metadata tmd_index[cnt];
    Index* tree_index[cnt] ={nullptr};
    int reference[s.num_of_cols];//colIndex-->treeIndex
    
    for (int i = 0; i < s.num_of_cols; i++)
    reference[i] = -1;
    
    for(int i = 0, j = 0; i < o.sc.set_cols ; i++){
        string col_name = o.sc.column_names[i];
        int col_no = s.getColumnIndex(col_name);
        
        if(s.getColumnIndexType(col_name)==2){
            dm_index[j].open_index(o.table_name, col_name);
            tree_index[j] = new Index(dm_index[j],tmd_index[j],s.getColumnType(col_no),sizeof(int),s.getColumnSize(col_no));
            tree_index[j]->open_index();
            reference[s.getColumnIndex(col_name)]=j;
            j++;
        }
    }
    
    int key_off = s.getPkOffset();
    int main_row_size = s.row_size;
    bool oper = true;

    
    char key_main[sizeof(int)];
    dh.converter(key_main, acc.search_value,int32,sizeof(int));
    where_clause wh;

    //updating.....
    try{
        if(acc.forward){
        tree.scan_forward(main_row_size, key_main, key_off,[&](const char* ptr_to_main){ 
            if(wh.evaluvate_tree(o.root, s, (unsigned char*)ptr_to_main)){
            char* key_main_ptr = (char*)ptr_to_main + key_off;

            for(int i = 0 ; i < cnt ; i++){
                //update into index files
                int col_no = s.getColumnIndex(o.sc.column_names[i]);

                if(reference[col_no] == -1){
                //update into main file
                int index_size = s.getColumnSize(col_no);
                int index_off = s.getColumnOffset(col_no);
                char new_index_key[index_size];
                dh.converter(new_index_key, o.sc.column_values[i], s.getColumnType(col_no), index_size);
                tree.update_row(key_main_ptr,main_row_size,key_off,index_off,index_size,new_index_key);
                }
                else{
                int index_size = s.getColumnSize(col_no);
                int index_row_size = index_size + sizeof(int);
                int index_off = s.getColumnOffset(col_no);

                char old_row_index[index_row_size];
                char new_row_index[index_row_size];
                char new_index_key[index_size];
                char* old_index_ptr = (char*)ptr_to_main  + index_off;

                dh.converter(new_index_key, o.sc.column_values[i], s.getColumnType(col_no), index_size);

                dh.converter(old_row_index, old_index_ptr, key_main_ptr , index_size, sizeof(int));
                dh.converter(new_row_index, new_index_key, key_main_ptr , index_size, sizeof(int));

                tree_index[reference[col_no]]->update_index(old_row_index, new_row_index);

                //update into main file
                tree.update_row(key_main_ptr,main_row_size,key_off,index_off,index_size,new_index_key);
                }
            }}
        
        });}  
        else{
        tree.scan_backward(main_row_size, key_main, key_off,[&](const char* ptr_to_main){
            if(wh.evaluvate_tree(o.root, s, (unsigned char*)ptr_to_main)){
            char* key_main_ptr = (char*)ptr_to_main + key_off;

            for(int i = 0 ; i < cnt ; i++){
                //update into index files
                int col_no = s.getColumnIndex(o.sc.column_names[i]);

                if(reference[col_no] == -1){
                //update into main file
                int index_size = s.getColumnSize(col_no);
                int index_off = s.getColumnOffset(col_no);
                char new_index_key[index_size];
                dh.converter(new_index_key, o.sc.column_values[i], s.getColumnType(col_no), index_size);
                tree.update_row(key_main_ptr,main_row_size,key_off,index_off,index_size,new_index_key);
                }
                else{
                int index_size = s.getColumnSize(col_no);
                int index_row_size = index_size + sizeof(int);
                int index_off = s.getColumnOffset(col_no);

                char old_row_index[index_row_size];
                char new_row_index[index_row_size];
                char new_index_key[index_size];
                char* old_index_ptr = (char*)ptr_to_main  + index_off;

                dh.converter(new_index_key, o.sc.column_values[i], s.getColumnType(col_no), index_size);

                dh.converter(old_row_index, old_index_ptr, key_main_ptr , index_size, sizeof(int));
                dh.converter(new_row_index, new_index_key, key_main_ptr , index_size, sizeof(int));

                tree_index[reference[col_no]]->update_index(old_row_index, new_row_index);

                //update into main file
                tree.update_row(key_main_ptr,main_row_size,key_off,index_off,index_size,new_index_key);
                }
            }}
        });}  
    }
    catch(DB_error &e){
        cout<<e.message<<endl;
        oper = false;
    }

    for (int i = 0; i < cnt; i++) {
        delete tree_index[i];
    }
    
    if(oper)
    cout<<"data updated into : " + o.table_name<<endl;
    return;
}

void executor::execute_update_skrange(update_operation &o, AccessPath &acc){
    string file_name = "..\\data\\main_"  + o.table_name + ".tbl";

    // handle catalog
    catalog cat;
    if (!cat.db_exists("..\\data\\main.catalog")) {
        o.error = DB_error(ERR_UNKNOWN_TABLE, "catalog not found: ");
        cout<<o.error.message<<endl;
        return;
    }
    o.error = cat.load_catalog("main");
    if(!o.error.ok()){
        cout<<o.error.message<<endl;
        return;
    }
    if(!cat.has_table(o.table_name)){
        o.error = DB_error(ERR_RUNTIME,"table " + o.table_name + " does not exists");
        cout<<o.error.message<<endl;
        return;
    }
 
    //handle schema
    schema s;
    o.error = s.load_schema("main",o.table_name);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //set verification
    DataHandler dh;
    o.error = dh.set_verify(s,o.sc);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //load the main file
    Disk_Manager dm_main;
    Table_Metadata tmd_main;
    
    dm_main.open_file(o.table_name);
    BplusTree tree(dm_main, tmd_main,int32, sizeof(int),true);
    tree.open_tree();
    
    
    //load the index files
    int cnt = o.sc.set_cols;
    
    Disk_Manager dm_index[cnt];
    Table_Metadata tmd_index[cnt];
    Index* tree_index[cnt] ={nullptr};
    int reference[s.num_of_cols];//colIndex-->treeIndex
    
    for (int i = 0; i < s.num_of_cols; i++)
    reference[i] = -1;
    
    for(int i = 0, j = 0; i < o.sc.set_cols ; i++){
        string col_name = o.sc.column_names[i];
        int col_no = s.getColumnIndex(col_name);
        
        if(s.getColumnIndexType(col_name)==2){
            dm_index[j].open_index(o.table_name, col_name);
            tree_index[j] = new Index(dm_index[j],tmd_index[j],s.getColumnType(col_no),sizeof(int),s.getColumnSize(col_no));
            tree_index[j]->open_index();
            reference[s.getColumnIndex(col_name)]=j;
            j++;
        }
    }

    //load the index file which is to scan
    Disk_Manager dm_index_scan;
    Table_Metadata tmd_index_scan;
    Index* tree_index_scan;

    int col_no = s.getColumnIndex(acc.col_name);
    int ref = reference[col_no];
    if(ref == -1){
        dm_index_scan.open_index(o.table_name, acc.col_name);
        tree_index_scan = new Index(dm_index_scan,tmd_index_scan,s.getColumnType(col_no),sizeof(int),s.getColumnSize(col_no));
        tree_index_scan->open_index();
    }
    else{
        tree_index_scan = tree_index[ref];
    }
    
    where_clause wh;
    datatype dt_index_scan = s.getColumnType(col_no);
    int index_scan_size = s.getColumnSize(col_no);
    int key_off = s.getPkOffset();
    int main_row_size = s.row_size;
    char key_scan[index_scan_size];
    dh.converter(key_scan, acc.search_value,dt_index_scan,index_scan_size);
    bool oper = true;
    cout<<dt_index_scan<<endl;
    string str(key_scan, index_scan_size);
    cout << str<<endl;

    vector<primary_key> pks;
    
    //updating.....
    try{
        if(acc.forward){
        tree_index_scan->find_all_pks_forward(key_scan,dt_index_scan,[&](const char* ptr_to_index_scan){
            primary_key pk_store;
            memcpy(&pk_store.pk, ptr_to_index_scan, sizeof(int));
            pks.push_back(pk_store);  
        });}
        else{
        tree_index_scan->find_all_pks_backward(key_scan,dt_index_scan,[&](const char* ptr_to_index_scan){
            primary_key pk_store;
            memcpy(&pk_store.pk, ptr_to_index_scan, sizeof(int));
            pks.push_back(pk_store);  
        });}

        for(int i = 0 ; i<pks.size() ; i++){
        char* key_main = pks[i].pk;
        tree.scan_point(main_row_size,key_main,key_off,[&](const char* ptr_to_main){
            if(wh.evaluvate_tree(o.root, s, (unsigned char*)ptr_to_main)){
            for(int i = 0 ; i < cnt ; i++){
            int col_no = s.getColumnIndex(o.sc.column_names[i]);

            if(reference[col_no] == -1){
            //update into main file
            int index_size = s.getColumnSize(col_no);
            int index_off = s.getColumnOffset(col_no);
            char new_index_key[index_size];
            dh.converter(new_index_key, o.sc.column_values[i], s.getColumnType(col_no), index_size);
            tree.update_row(key_main,main_row_size,key_off,index_off,index_size,new_index_key);
            }
            else{
            //updateinto index fils
            int index_size = s.getColumnSize(col_no);
            int index_row_size = index_size + sizeof(int);
            int index_off = s.getColumnOffset(col_no);

            char old_row_index[index_row_size];
            char new_row_index[index_row_size];
            char new_index_key[index_size];
            char* old_index_ptr = (char*)ptr_to_main  + index_off;

            dh.converter(new_index_key, o.sc.column_values[i], s.getColumnType(col_no), index_size);

            dh.converter(old_row_index, old_index_ptr, key_main , index_size, sizeof(int));
            dh.converter(new_row_index, new_index_key, key_main , index_size, sizeof(int));

            tree_index[reference[col_no]]->update_index(old_row_index, new_row_index);

            //update into main file
            tree.update_row(key_main,main_row_size,key_off,index_off,index_size,new_index_key);
            }
        }}});}
    }
    catch(DB_error &e){
        cout<<e.message<<endl;
        oper = false;
    }

    for (int i = 0; i < cnt; i++) {
        delete tree_index[i];
    }
    
    if(oper)
    cout<<"data updated into : " + o.table_name<<endl;
    return;
}

void executor::execute_update_fullscan(update_operation &o, AccessPath &acc){
    string file_name = "..\\data\\main_"  + o.table_name + ".tbl";

    // handle catalog
    catalog cat;
    if (!cat.db_exists("..\\data\\main.catalog")) {
        o.error = DB_error(ERR_UNKNOWN_TABLE, "catalog not found: ");
        cout<<o.error.message<<endl;
        return;
    }
    o.error = cat.load_catalog("main");
    if(!o.error.ok()){
        cout<<o.error.message<<endl;
        return;
    }
    if(!cat.has_table(o.table_name)){
        o.error = DB_error(ERR_RUNTIME,"table " + o.table_name + " does not exists");
        cout<<o.error.message<<endl;
        return;
    }
 
    //handle schema
    schema s;
    o.error = s.load_schema("main",o.table_name);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //set verification
    DataHandler dh;
    o.error = dh.set_verify(s,o.sc);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //load the main file
    Disk_Manager dm_main;
    Table_Metadata tmd_main;
    
    dm_main.open_file(o.table_name);
    BplusTree tree(dm_main, tmd_main,int32, sizeof(int),true);
    tree.open_tree();
    
    
    //load the index files
    int cnt = o.sc.set_cols;
    
    Disk_Manager dm_index[cnt];
    Table_Metadata tmd_index[cnt];
    Index* tree_index[cnt] ={nullptr};
    int reference[s.num_of_cols];//colIndex-->treeIndex
    
    for (int i = 0; i < s.num_of_cols; i++)
    reference[i] = -1;
    
    for(int i = 0, j = 0; i < o.sc.set_cols ; i++){
        string col_name = o.sc.column_names[i];
        int col_no = s.getColumnIndex(col_name);
        
        if(s.getColumnIndexType(col_name)==2){
            dm_index[j].open_index(o.table_name, col_name);
            tree_index[j] = new Index(dm_index[j],tmd_index[j],s.getColumnType(col_no),sizeof(int),s.getColumnSize(col_no));
            tree_index[j]->open_index();
            reference[s.getColumnIndex(col_name)]=j;
            j++;
        }
    }
    
    where_clause wh;
    int key_off = s.getPkOffset();
    int main_row_size = s.row_size;
    bool oper = true;

    
    //updating.....
    try{
        tree.scan_all(main_row_size,[&](const char* ptr_to_main){
            if(wh.evaluvate_tree(o.root, s, (unsigned char*)ptr_to_main)){
            char* key_main = (char*)ptr_to_main + key_off;

            for(int i = 0 ; i < cnt ; i++){
                //update into index files
                int col_no = s.getColumnIndex(o.sc.column_names[i]);

                if(reference[col_no] == -1){
                //update into main file
                int index_size = s.getColumnSize(col_no);
                int index_off = s.getColumnOffset(col_no);
                char new_index_key[index_size];
                dh.converter(new_index_key, o.sc.column_values[i], s.getColumnType(col_no), index_size);
                tree.update_row(key_main,main_row_size,key_off,index_off,index_size,new_index_key);
                }
                else{
                //updateinto index fils
                int index_size = s.getColumnSize(col_no);
                int index_row_size = index_size + sizeof(int);
                int index_off = s.getColumnOffset(col_no);

                char old_row_index[index_row_size];
                char new_row_index[index_row_size];
                char new_index_key[index_size];
                char* old_index_ptr = (char*)ptr_to_main  + index_off;

                dh.converter(new_index_key, o.sc.column_values[i], s.getColumnType(col_no), index_size);

                dh.converter(old_row_index, old_index_ptr, key_main , index_size, sizeof(int));
                dh.converter(new_row_index, new_index_key, key_main , index_size, sizeof(int));

                tree_index[reference[col_no]]->update_index(old_row_index, new_row_index);

                //update into main file
                tree.update_row(key_main,main_row_size,key_off,index_off,index_size,new_index_key);
                }
            }}
        
        });  
    }
    catch(DB_error &e){
        cout<<e.message<<endl;
        oper = false;
    }

    for (int i = 0; i < cnt; i++) {
        delete tree_index[i];
    }
    
    if(oper)
    cout<<"data updated into : " + o.table_name<<endl;
    return;
}

void executor::execute_delete_pklookup(delete_operation &o, AccessPath &acc){
    string file_name = "..\\data\\main_"  + o.table_name + ".tbl";

    // handle catalog
    catalog cat;
    if (!cat.db_exists("..\\data\\main.catalog")) {
        o.error = DB_error(ERR_UNKNOWN_TABLE, "catalog not found: ");
        cout<<o.error.message<<endl;
        return;
    }
    o.error = cat.load_catalog("main");
    if(!o.error.ok()){
        cout<<o.error.message<<endl;
        return;
    }
    if(!cat.has_table(o.table_name)){
        o.error = DB_error(ERR_RUNTIME,"table " + o.table_name + " does not exists");
        cout<<o.error.message<<endl;
        return;
    }
 
    //handle schema
    schema s;
    o.error = s.load_schema("main",o.table_name);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //load the main file
    Disk_Manager dm_main;
    Table_Metadata tmd_main;
    
    dm_main.open_file(o.table_name);
    BplusTree tree(dm_main, tmd_main,int32, sizeof(int),true);
    tree.open_tree();
    
    
    //load the index files
    int cnt = s.getIndexCount();
    
    Disk_Manager dm_index[cnt];
    Table_Metadata tmd_index[cnt];
    Index* tree_index[cnt];
    int reference[s.num_of_cols];//colIndex-->treeIndex
 
    for (int i = 0; i < s.num_of_cols; i++)
    reference[i] = -1;
    
    for(int i = 0, j = 0; i < s.num_of_cols ; i++){
        if(s.getColumnIndexType(i)==2){
            dm_index[j].open_index(o.table_name, s.column_name[i]);
            tree_index[j] = new Index(dm_index[j],tmd_index[j],s.getColumnType(i),sizeof(int),s.getColumnSize(i));
            tree_index[j]->open_index();
            reference[i]=j;
            j++;
        }
    }
    
    
    //read the index file
    DataHandler dh;
    where_clause wh;
    int primary_row_size = s.row_size;
    int key_off = s.getPkOffset();
    char search_key[sizeof(int)];
    dh.converter(search_key, acc.search_value, int32, sizeof(int));

    try{
        bool this_row_deletion = false;

        tree.scan_point(primary_row_size,search_key,key_off, [&](const char* ptr_to_prim){
            if(wh.evaluvate_tree(o.root, s, (const unsigned char*)ptr_to_prim)){
            this_row_deletion = true;

            //del all indexes present
            for(int i = 0; i < s.num_of_cols ; i++){
                int ref = reference[i];

                if(ref==-1)
                continue;

                int index_off = s.getColumnOffset(i);
                int index_size = s.getColumnSize(i);
                char* index_ptr = (char*)ptr_to_prim + index_off;
                char index_row[index_size]; 

                dh.converter(index_row,index_ptr,search_key,index_size,sizeof(int));

                tree_index[ref]->delete_index(index_row);
            }}
        });

        if(this_row_deletion){
            tree.delete_row(primary_row_size,search_key,key_off);
        }
    }
    catch(DB_error &db){
        cout<<db.message<<endl;
        return;
    }

    for(int i = 0 ; i<cnt ; i++){
        delete tree_index[i];
    }

    cout<<"Data deleted from : "<< o.table_name <<endl;
}

void executor::execute_delete_sklookup(delete_operation &o, AccessPath &acc){
    string file_name = "..\\data\\main_"  + o.table_name + ".tbl";

    // handle catalog
    catalog cat;
    if (!cat.db_exists("..\\data\\main.catalog")) {
        o.error = DB_error(ERR_UNKNOWN_TABLE, "catalog not found: ");
        cout<<o.error.message<<endl;
        return;
    }
    o.error = cat.load_catalog("main");
    if(!o.error.ok()){
        cout<<o.error.message<<endl;
        return;
    }
    if(!cat.has_table(o.table_name)){
        o.error = DB_error(ERR_RUNTIME,"table " + o.table_name + " does not exists");
        cout<<o.error.message<<endl;
        return;
    }
 
    //handle schema
    schema s;
    o.error = s.load_schema("main",o.table_name);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //load the main file
    Disk_Manager dm_main;
    Table_Metadata tmd_main;
    
    dm_main.open_file(o.table_name);
    BplusTree tree(dm_main, tmd_main,int32, sizeof(int),true);
    tree.open_tree();
    
    
    //load the index files
    int cnt = s.getIndexCount();
    
    Disk_Manager dm_index[cnt];
    Table_Metadata tmd_index[cnt];
    Index* tree_index[cnt];
    int reference[s.num_of_cols];//colIndex-->treeIndex
 
    for (int i = 0; i < s.num_of_cols; i++)
    reference[i] = -1;
    
    for(int i = 0, j = 0; i < s.num_of_cols ; i++){
        if(s.getColumnIndexType(i)==2){
            dm_index[j].open_index(o.table_name, s.column_name[i]);
            tree_index[j] = new Index(dm_index[j],tmd_index[j],s.getColumnType(i),sizeof(int),s.getColumnSize(i));
            tree_index[j]->open_index();
            reference[i]=j;
            j++;
        }
    }

    //assign the index scan 
    Disk_Manager dm_index_scan;
    Table_Metadata tmd_index_scan;
    Index* tree_index_scan;
    int col_no = s.getColumnIndex(acc.col_name);
    datatype dt_scan = s.getColumnType(col_no);
    int index_scan_size = s.getColumnSize(col_no);

    tree_index_scan = tree_index[reference[col_no]];

    
    //read the index file
    DataHandler dh;
    where_clause wh;
    int primary_row_size = s.row_size;
    int key_off = s.getPkOffset();
    int index_row_size = index_scan_size + sizeof(int);
    char search_key[index_scan_size];
    dh.converter(search_key, acc.search_value, dt_scan, index_scan_size); 

    vector<primary_key> pks;

    try{
        //get all pks corresponding tot the secondary key
        tree_index_scan->find_all_pks(search_key, dt_scan, [&](const char* ptr_to_index_scan){
            primary_key pk_store;
            memcpy(pk_store.pk, ptr_to_index_scan, sizeof(int));
            pks.push_back(pk_store);
        });
        
        //using main tree delete from secondary keys table
        for(int i = 0 ; i < pks.size() ; i++){
        tree.scan_point(primary_row_size,pks[i].pk,key_off, [&](const char* ptr_to_prim){
            if(wh.evaluvate_tree(o.root, s, (const unsigned char*)ptr_to_prim)){
            //del all indexes present
            for(int i = 0; i < s.num_of_cols ; i++){
                int ref = reference[i];

                if(ref==-1)
                continue;

                int index_off = s.getColumnOffset(i);
                int index_size = s.getColumnSize(i);
                char* index_ptr = (char*)ptr_to_prim + index_off;
                char index_row[index_size]; 
                char* key_main = (char*) ptr_to_prim + key_off; 

                dh.converter(index_row,index_ptr,key_main,index_size,sizeof(int));

                tree_index[ref]->delete_index(index_row);
            }}
        });}

        //delete rows in main tree
        for(int i = 0 ; i < pks.size() ; i++){
            tree.delete_row(primary_row_size, pks[i].pk, key_off);
        }
    }
    catch(DB_error &db){
        cout<<db.message<<endl;
        return;
    }

    for(int i = 0 ; i<cnt ; i++){
        delete tree_index[i];
    }

    cout<<"Data deleted from : "<< o.table_name <<endl;
}

void executor::execute_delete_pkrange(delete_operation &o, AccessPath &acc){
    string file_name = "..\\data\\main_"  + o.table_name + ".tbl";

    // handle catalog
    catalog cat;
    if (!cat.db_exists("..\\data\\main.catalog")) {
        o.error = DB_error(ERR_UNKNOWN_TABLE, "catalog not found: ");
        cout<<o.error.message<<endl;
        return;
    }
    o.error = cat.load_catalog("main");
    if(!o.error.ok()){
        cout<<o.error.message<<endl;
        return;
    }
    if(!cat.has_table(o.table_name)){
        o.error = DB_error(ERR_RUNTIME,"table " + o.table_name + " does not exists");
        cout<<o.error.message<<endl;
        return;
    }
 
    //handle schema
    schema s;
    o.error = s.load_schema("main",o.table_name);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //load the main file
    Disk_Manager dm_main;
    Table_Metadata tmd_main;
    
    dm_main.open_file(o.table_name);
    BplusTree tree(dm_main, tmd_main,int32, sizeof(int),true);
    tree.open_tree();
    
    
    //load the index files
    int cnt = s.getIndexCount();
    
    Disk_Manager dm_index[cnt];
    Table_Metadata tmd_index[cnt];
    Index* tree_index[cnt];
    int reference[s.num_of_cols];//colIndex-->treeIndex
 
    for (int i = 0; i < s.num_of_cols; i++)
    reference[i] = -1;
    
    for(int i = 0, j = 0; i < s.num_of_cols ; i++){
        if(s.getColumnIndexType(i)==2){
            dm_index[j].open_index(o.table_name, s.column_name[i]);
            tree_index[j] = new Index(dm_index[j],tmd_index[j],s.getColumnType(i),sizeof(int),s.getColumnSize(i));
            tree_index[j]->open_index();
            reference[i]=j;
            j++;
        }
    }
    
    
    //read the index file
    DataHandler dh;
    where_clause wh;
    int primary_row_size = s.row_size;
    int key_off = s.getPkOffset();
    char search_key[sizeof(int)];
    dh.converter(search_key, acc.search_value, int32, sizeof(int));

    vector<primary_key> pks;

    try{
        if(acc.forward){
        tree.scan_forward(primary_row_size,search_key,key_off, [&](const char* ptr_to_prim){
            if(wh.evaluvate_tree(o.root, s, (const unsigned char*)ptr_to_prim)){
            //store the pk
            primary_key pk_store;
            memcpy(pk_store.pk, ptr_to_prim + key_off , sizeof(int));
            pks.push_back(pk_store);

            //del all indexes present
            for(int i = 0; i < s.num_of_cols ; i++){
                int ref = reference[i];

                if(ref==-1)
                continue;

                int index_off = s.getColumnOffset(i);
                int index_size = s.getColumnSize(i);
                char* index_ptr = (char*)ptr_to_prim + index_off;
                char index_row[index_size]; 
                char* key_main = (char*) ptr_to_prim + key_off; 

                dh.converter(index_row,index_ptr,key_main,index_size,sizeof(int));

                tree_index[ref]->delete_index(index_row);
            }}
        });}
        else{
        tree.scan_backward(primary_row_size,search_key,key_off, [&](const char* ptr_to_prim){
            if(wh.evaluvate_tree(o.root, s, (const unsigned char*)ptr_to_prim)){
            //store the pk
            primary_key pk_store;
            memcpy(pk_store.pk, ptr_to_prim + key_off , sizeof(int));
            pks.push_back(pk_store);

            //del all indexes present
            for(int i = 0; i < s.num_of_cols ; i++){
                int ref = reference[i];

                if(ref==-1)
                continue;

                int index_off = s.getColumnOffset(i);
                int index_size = s.getColumnSize(i);
                char* index_ptr = (char*)ptr_to_prim + index_off;
                char index_row[index_size]; 
                char* key_main = (char*) ptr_to_prim + key_off; 

                dh.converter(index_row,index_ptr,key_main,index_size,sizeof(int));

                tree_index[ref]->delete_index(index_row);
            }}
        });}

        //delete rows in main tree
        for(int i = 0 ; i < pks.size() ; i++){
            tree.delete_row(primary_row_size, pks[i].pk, key_off);
        }
    }
    catch(DB_error &db){
        cout<<db.message<<endl;
        return;
    }

    for(int i = 0 ; i<cnt ; i++){
        delete tree_index[i];
    }

    cout<<"Data deleted from : "<< o.table_name <<endl;
}

void executor::execute_delete_skrange(delete_operation &o, AccessPath &acc){
    string file_name = "..\\data\\main_"  + o.table_name + ".tbl";

    // handle catalog
    catalog cat;
    if (!cat.db_exists("..\\data\\main.catalog")) {
        o.error = DB_error(ERR_UNKNOWN_TABLE, "catalog not found: ");
        cout<<o.error.message<<endl;
        return;
    }
    o.error = cat.load_catalog("main");
    if(!o.error.ok()){
        cout<<o.error.message<<endl;
        return;
    }
    if(!cat.has_table(o.table_name)){
        o.error = DB_error(ERR_RUNTIME,"table " + o.table_name + " does not exists");
        cout<<o.error.message<<endl;
        return;
    }
 
    //handle schema
    schema s;
    o.error = s.load_schema("main",o.table_name);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //load the main file
    Disk_Manager dm_main;
    Table_Metadata tmd_main;
    
    dm_main.open_file(o.table_name);
    BplusTree tree(dm_main, tmd_main,int32, sizeof(int),true);
    tree.open_tree();
    
    
    //load the index files
    int cnt = s.getIndexCount();
    
    Disk_Manager dm_index[cnt];
    Table_Metadata tmd_index[cnt];
    Index* tree_index[cnt];
    int reference[s.num_of_cols];//colIndex-->treeIndex
 
    for (int i = 0; i < s.num_of_cols; i++)
    reference[i] = -1;
    
    for(int i = 0, j = 0; i < s.num_of_cols ; i++){
        if(s.getColumnIndexType(i)==2){
            dm_index[j].open_index(o.table_name, s.column_name[i]);
            tree_index[j] = new Index(dm_index[j],tmd_index[j],s.getColumnType(i),sizeof(int),s.getColumnSize(i));
            tree_index[j]->open_index();
            reference[i]=j;
            j++;
        }
    }

    //assign the index scan 
    Disk_Manager dm_index_scan;
    Table_Metadata tmd_index_scan;
    Index* tree_index_scan;
    int col_no = s.getColumnIndex(acc.col_name);
    datatype dt_scan = s.getColumnType(col_no);
    int index_scan_size = s.getColumnSize(col_no);

    tree_index_scan = tree_index[reference[col_no]];

    
    //read the index file
    DataHandler dh;
    where_clause wh;
    int primary_row_size = s.row_size;
    int key_off = s.getPkOffset();
    int index_row_size = index_scan_size + sizeof(int);
    char search_key[index_scan_size];
    dh.converter(search_key, acc.search_value, dt_scan, index_scan_size); 

    vector<primary_key> pks;

    try{
        if(acc.forward){
        tree_index_scan->find_all_pks_forward(search_key, dt_scan, [&](const char* ptr_to_index_scan){
            primary_key pk_store;
            memcpy(pk_store.pk, ptr_to_index_scan, sizeof(int));
            pks.push_back(pk_store);
        });
        
        for(int i = 0 ; i < pks.size() ; i++){
        tree.scan_point(primary_row_size,pks[i].pk,key_off, [&](const char* ptr_to_prim){
            if(wh.evaluvate_tree(o.root, s, (const unsigned char*)ptr_to_prim)){
            //del all indexes present
            for(int i = 0; i < s.num_of_cols ; i++){
                int ref = reference[i];

                if(ref==-1)
                continue;

                int index_off = s.getColumnOffset(i);
                int index_size = s.getColumnSize(i);
                char* index_ptr = (char*)ptr_to_prim + index_off;
                char index_row[index_size]; 
                char* key_main = (char*) ptr_to_prim + key_off; 

                dh.converter(index_row,index_ptr,key_main,index_size,sizeof(int));

                tree_index[ref]->delete_index(index_row);
            }}
        });}}
        else{
        tree_index_scan->find_all_pks_backward(search_key, dt_scan, [&](const char* ptr_to_index_scan){
            primary_key pk_store;
            memcpy(pk_store.pk, ptr_to_index_scan, sizeof(int));
            pks.push_back(pk_store);
        });
        
        for(int i = 0 ; i < pks.size() ; i++){
        tree.scan_point(primary_row_size,pks[i].pk,key_off, [&](const char* ptr_to_prim){
            if(wh.evaluvate_tree(o.root, s, (const unsigned char*)ptr_to_prim)){
            //del all indexes present
            for(int i = 0; i < s.num_of_cols ; i++){
                int ref = reference[i];

                if(ref==-1)
                continue;

                int index_off = s.getColumnOffset(i);
                int index_size = s.getColumnSize(i);
                char* index_ptr = (char*)ptr_to_prim + index_off;
                char index_row[index_size]; 
                char* key_main = (char*) ptr_to_prim + key_off; 

                dh.converter(index_row,index_ptr,key_main,index_size,sizeof(int));

                tree_index[ref]->delete_index(index_row);
            }}
        });}}

        //delete rows in main tree
        for(int i = 0 ; i < pks.size() ; i++){
            tree.delete_row(primary_row_size, pks[i].pk, key_off);
        }
    }
    catch(DB_error &db){
        cout<<db.message<<endl;
        return;
    }

    for(int i = 0 ; i<cnt ; i++){
        delete tree_index[i];
    }

    cout<<"Data deleted from : "<< o.table_name <<endl;

}

void executor::execute_delete_fullscan(delete_operation &o, AccessPath &acc){
    string file_name = "..\\data\\main_"  + o.table_name + ".tbl";

    // handle catalog
    catalog cat;
    if (!cat.db_exists("..\\data\\main.catalog")) {
        o.error = DB_error(ERR_UNKNOWN_TABLE, "catalog not found: ");
        cout<<o.error.message<<endl;
        return;
    }
    o.error = cat.load_catalog("main");
    if(!o.error.ok()){
        cout<<o.error.message<<endl;
        return;
    }
    if(!cat.has_table(o.table_name)){
        o.error = DB_error(ERR_RUNTIME,"table " + o.table_name + " does not exists");
        cout<<o.error.message<<endl;
        return;
    }
 
    //handle schema
    schema s;
    o.error = s.load_schema("main",o.table_name);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }

    //load the main file
    Disk_Manager dm_main;
    Table_Metadata tmd_main;
    
    dm_main.open_file(o.table_name);
    BplusTree tree(dm_main, tmd_main,int32, sizeof(int),true);
    tree.open_tree();
    
    
    //load the index files
    int cnt = s.getIndexCount();
    
    Disk_Manager dm_index[cnt];
    Table_Metadata tmd_index[cnt];
    Index* tree_index[cnt];
    int reference[s.num_of_cols];//colIndex-->treeIndex
 
    for (int i = 0; i < s.num_of_cols; i++)
    reference[i] = -1;
    
    for(int i = 0, j = 0; i < s.num_of_cols ; i++){
        if(s.getColumnIndexType(i)==2){
            dm_index[j].open_index(o.table_name, s.column_name[i]);
            tree_index[j] = new Index(dm_index[j],tmd_index[j],s.getColumnType(i),sizeof(int),s.getColumnSize(i));
            tree_index[j]->open_index();
            reference[i]=j;
            j++;
        }
    }
    
    
    //read the index file
    DataHandler dh;
    where_clause wh;
    int primary_row_size = s.row_size;
    int key_off = s.getPkOffset();

    vector<primary_key> pks;

    try{
        tree.scan_all(primary_row_size, [&](const char* ptr_to_prim){
            if(wh.evaluvate_tree(o.root, s, (const unsigned char*)ptr_to_prim)){
            //store the pk
            primary_key pk_store;
            memcpy(pk_store.pk, ptr_to_prim + key_off , sizeof(int));
            pks.push_back(pk_store);

            //del all indexes present
            for(int i = 0; i < s.num_of_cols ; i++){
                int ref = reference[i];

                if(ref==-1)
                continue;

                int index_off = s.getColumnOffset(i);
                int index_size = s.getColumnSize(i);
                char* index_ptr = (char*)ptr_to_prim + index_off;
                char index_row[index_size]; 
                char* search_key = (char*)ptr_to_prim + key_off;

                dh.converter(index_row,index_ptr,search_key,index_size,sizeof(int));

                tree_index[ref]->delete_index(index_row);
            }}
        });
        
        //delete rows in main tree
        for(int i = 0 ; i < pks.size() ; i++){
            tree.delete_row(primary_row_size, pks[i].pk, key_off);
        }
    }
    catch(DB_error &db){
        cout<<db.message<<endl;
        return;
    }

    for(int i = 0 ; i<cnt ; i++){
        delete tree_index[i];
    }

    cout<<"Data deleted from : "<< o.table_name <<endl;
}

void executor::execute_drop_cluster(select_operation &o, AccessPath &acc){
    string file_name = "..\\data\\main_"  + o.table_name + ".tbl";

    if (!o.error.ok()) {
        cout << o.error.message << endl;
        return;
    }

    // handle catalog
    catalog cat;
    if (!cat.db_exists("..\\data\\main.catalog")) {
        o.error = DB_error(ERR_UNKNOWN_TABLE, "catalog not found: ");
        cout<<o.error.message<<endl;
        return;
    }
    o.error = cat.load_catalog("main");
    if(!o.error.ok()){
        cout<<o.error.message<<endl;
        return;
    }
    o.error = cat.remove_table(o.table_name, "main"); 
    if (!o.error.ok()) {
        cout << o.error.message << endl;
        return;
    }
    cat.save_catalog("main");

    //handle schema
    schema s;
    o.error = s.load_schema("main",o.table_name);
    if (!o.error.ok()) {
        cout<<o.error.message<<endl;
        return;
    }


    //remove index files
    for(int i = 0 ; i < s.num_of_cols ; i++){
        if(s.getColumnIndexType(i)==2){
            string file = "..\\data\\main_" + o.table_name + "_" + s.column_name[i] + ".tbl";
            ::remove(file.c_str());
    }}

    //delete main file
    string file = "..\\data\\main_" + o.table_name  + ".tbl";
    ::remove(file.c_str());

    //delete scheman file
    file = "..\\data\\main_" + o.table_name  + ".schema";
    ::remove(file.c_str());

    cout<<"table deleted : " + o.table_name<<endl;
}
