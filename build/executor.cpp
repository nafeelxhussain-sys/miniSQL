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
        if(acc_path.type==1) execute_select_pklookup(o.select);
        else if(acc_path.type==2) execute_select_sklookup(o.select);
        else if(acc_path.type==3) execute_select_pkrange(o.select);
        else if(acc_path.type==4) execute_select_skrange(o.select);
        else if(acc_path.type==5) execute_select_fullscan(o.select);
        else if(acc_path.type==7) execute_select_heap(o.select);
    }

    else if (o.operation_type == "UPDATE") {
        if(acc_path.type==1) execute_update_pklookup(o.update);
        else if(acc_path.type==2) execute_update_sklookup(o.update);
        else if(acc_path.type==3) execute_update_pkrange(o.update);
        else if(acc_path.type==4) execute_update_skrange(o.update);
        else if(acc_path.type==5) execute_update_fullscan(o.update);
        else if(acc_path.type==7) execute_update_heap(o.update);
    }

    else if (o.operation_type == "DELETE") {
        if(acc_path.type==1) execute_delete_pklookup(o.delete_);
        else if(acc_path.type==2) execute_delete_sklookup(o.delete_);
        else if(acc_path.type==3) execute_delete_pkrange(o.delete_);
        else if(acc_path.type==4) execute_delete_skrange(o.delete_);
        else if(acc_path.type==5) execute_delete_fullscan(o.delete_);
        else if(acc_path.type==7) execute_delete_heap(o.delete_);
    }

    else if (o.operation_type == "DROP") {
        if(acc_path.type!=heap)execute_drop_cluster(o.select);
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
    }

    for (int i = 0; i < cnt; i++) {
        delete tree_index[i];
    }
    
    cout<<"data inserted into : " + o.table_name<<endl;
    return;
}

void executor::execute_select_pklookup(select_operation &o){}
void executor::execute_select_sklookup(select_operation &o){}
void executor::execute_select_pkrange(select_operation &o){}
void executor::execute_select_skrange(select_operation &o){}
void executor::execute_select_fullscan(select_operation &o){}

void executor::execute_update_pklookup(update_operation &o){}
void executor::execute_update_sklookup(update_operation &o){}
void executor::execute_update_pkrange(update_operation &o){}
void executor::execute_update_skrange(update_operation &o){}
void executor::execute_update_fullscan(update_operation &o){}

void executor::execute_delete_pklookup(delete_operation &o){}
void executor::execute_delete_sklookup(delete_operation &o){}
void executor::execute_delete_pkrange(delete_operation &o){}
void executor::execute_delete_skrange(delete_operation &o){}
void executor::execute_delete_fullscan(delete_operation &o){}

void executor::execute_drop_cluster(select_operation &o){}
