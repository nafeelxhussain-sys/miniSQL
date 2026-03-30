#include "core.h"
#include "error.h"
#include "utils.h"
#include "schema.h"
#include "filter.h"
#include "buffer.h"


// -------------------- catalog --------------------
void catalog::clear() {
    table_count = 0;
    for (int i = 0; i < MAX_TABLES; i++) {
        table_names[i] = "";
        coulumn_counts[i] = 0;
        table_clustered[i]=-1;
    }
}

catalog::catalog() {
    table_count = 0;
    for (int i = 0; i < MAX_TABLES; i++) {
        table_names[i] = "";
        coulumn_counts[i] = 0;
        table_clustered[i]=-1;
    }
}

bool catalog::db_exists(string path) {
    ifstream out(path, ios::binary);
    return out.is_open();
}

void catalog::save_catalog(string db_name) {
    string file_name = "..\\data\\" + db_name + ".catalog";
    this->db_name = db_name;

    if (db_exists(file_name)) {
        return;
    }

    ofstream out(file_name, ios::binary);
    uint32_t x = 0;
    out.write((char*)&x, sizeof(x));
    out.close();
}

DB_error catalog::load_catalog(string db_name) {
    string file_name = "..\\data\\" + db_name + ".catalog";

    if (!db_exists(file_name)) {
        return DB_error(ERR_NONE, "catalog does not exists ");
    }

    clear();

    ifstream in(file_name, ios::binary);
    in.seekg(0, ios::end);
    int size_catalog_data = in.tellg();
    in.seekg(0, ios::beg);

    unsigned char* buffer = new unsigned char[size_catalog_data];
    in.read((char*)buffer, size_catalog_data);
    in.close();

    memcpy(&table_count, buffer, sizeof(int));
    this->db_name = db_name;

    int start = 4;
    for (int i = 0; i < table_count; i++) {
        uint8_t x;
        memcpy(&x, buffer + start, sizeof(uint8_t));
        start++;

        table_names[i] = string((char*)(buffer + start), x);
        start += x;

        memcpy(&coulumn_counts[i], buffer + start, 2);
        start += 2;

        memcpy(&table_clustered[i], buffer+start,1);
        start+=1;
    }

    delete[] buffer;

    return DB_error(ERR_NONE,"");
}

DB_error catalog::add_table(string table_name, uint16_t coulumn_count, uint8_t is_clustered) {
    string file_name = "..\\data\\" + this->db_name + ".catalog";

    DB_error err(ERR_NONE, "");

    if (!db_exists(file_name)) {
        return DB_error(ERR_NONE, "catalog does not exists ");
    }

    if (has_table(table_name)) {
        return DB_error(ERR_RUNTIME, "table " + table_name + " already exists ");
    }

    ifstream in(file_name, ios::binary);
    in.seekg(0, ios::end);
    int size_catalog_data = in.tellg();
    in.seekg(0, ios::beg);

    int size_table_data = 4 + table_name.size();
    unsigned char* buffer = new unsigned char[size_catalog_data + size_table_data];
    in.read((char*)buffer, size_catalog_data);
    in.close();

    uint8_t x = table_name.size();
    memcpy(buffer + size_catalog_data, &x, 1);
    memcpy(buffer + size_catalog_data + 1, table_name.c_str(), x);

    uint16_t y = coulumn_count;
    memcpy(buffer + size_catalog_data + 1 + x, &y, 2);
    uint8_t z = is_clustered;
    memcpy(buffer + size_catalog_data + 3 + x, &z, 1);


    table_names[table_count] = table_name;
    coulumn_counts[table_count] = coulumn_count;
    table_clustered[table_count] = is_clustered;
    table_count++;

    memcpy(buffer, &table_count, sizeof(table_count));

    ofstream out(file_name, ios::binary);
    out.write((char*)buffer, size_catalog_data + size_table_data);
    out.close();
    delete[] buffer;

    return err;
}

bool catalog::has_table(string table_name) {
    for (int i = 0; i < table_count; i++) {
        if (table_names[i] == table_name) return true;
    }
    return false;
}

void catalog::print_catalog(string db_name) {
    cout<<db_name<<endl<<"|"<<endl;
    for (int i = 0; i < table_count; i++) {
        cout<<"|---- ";
        cout << table_names[i] << "  ( " << coulumn_counts[i] <<" columns )";
        if(table_clustered[i]==1) cout<<"  [clustered]"<<endl;
        else cout<<"  [heap]"<<endl;
    }
}

DB_error catalog::remove_table(string table_name,string db_name_){
    string file_name = "..\\data\\" + db_name_ + ".catalog";

    if (!db_exists(file_name)) {
        return DB_error(ERR_NONE, "catalog does not exists ");
    }

    clear();
    
    ifstream in(file_name, ios::binary);
    in.seekg(0, ios::end);
    int size_catalog_data = in.tellg();
    in.seekg(0, ios::beg);

    
    unsigned char* buffer = new unsigned char[size_catalog_data];
    in.read((char*)buffer, size_catalog_data);
    in.close();
    

    bool found = false;
    int deleted_size = 0;
    
    int start = 4;
    int end = 4;

    memcpy(&table_count, buffer, sizeof(int));

    for (int i = 0; i < table_count; i++) {
        
        uint8_t name_len;
        memcpy(&name_len, buffer + end, 1);
        end++;
        
        string table_name_((char*)(buffer + end), name_len);
        end += name_len + 3;
        
        if (table_name == table_name_) {
            
            deleted_size = end - start;
            
            memmove(buffer + start,
                buffer + end,
                size_catalog_data - end);
                
                found = true;
                break;
            }
            
            start = end;
        }
        
        if (!found) {
            delete[] buffer;

            return DB_error(ERR_NONE, "table not found");
        }
        

        size_catalog_data -= deleted_size;
        
        // update table count in header
        int new_count = table_count - 1;
        memcpy(buffer, &new_count, sizeof(int));
        
        ofstream out(file_name, ios::binary | ios::trunc);

        if (!out) {

            delete[] buffer;
            return DB_error(ERR_RUNTIME, "cannot open catalog for writing");
        }

        out.write((char*)buffer, size_catalog_data);
        out.close();
        
    delete[] buffer;

    return DB_error(ERR_NONE, "");
}


// -------------------- database --------------------
database::database(string name) {
    db_name = name;

    catalog c;
    if (!c.db_exists(db_name + ".catalog"))
        c.save_catalog(db_name);
}

bool database::table_exists(string tb_name) {
    string file_name = "..\\data\\" + this->db_name + '_' + tb_name + ".tbl";
    ifstream f(file_name);
    bool exists = f.is_open();
    f.close();
    return exists;
}

bool database::schema_exists(string tb_name) {
    string file_name = "..\\data\\" + this->db_name + '_' + tb_name + ".schema";
    ifstream f(file_name);
    bool exists = f.is_open();
    f.close();
    return exists;
}

DB_error database::create_table(string table_name, int num_of_cols, string* name, int* size, datatype* type, int *col_index) {
    string file_name = "..\\data\\" + this->db_name + '_' + table_name + ".tbl";

    DB_error err(ERR_NONE,"");
    if (table_exists(table_name)) {
        return DB_error(ERR_RUNTIME,"table " + table_name + " already exists");
    }

    catalog c;

    if(c.db_exists(this->db_name))
    c.save_catalog(this->db_name);

    err = c.load_catalog(db_name);
    if (!err.ok()) {
        return err;
    }

    err = c.add_table(table_name, num_of_cols,0);
    if (!err.ok()) {
        return err;
    }

    schema s;
    err = s.create_schema_file(this->db_name, table_name, num_of_cols, name, size, type, col_index,0);
    if (!err.ok()) {
        return err;
    }


    ofstream out(file_name);
    out.close();
    
    return err;
}

DB_error database::insert_into_table(string table_name, string* data, int size_of_data) {
    string file_name = "..\\data\\" + this->db_name + '_' + table_name + ".tbl";

    DB_error err(ERR_NONE,"");

    if (!table_exists(table_name) || !schema_exists(table_name)) {
        return DB_error(ERR_UNKNOWN_TABLE,"table " + table_name + " does not exists");
    }


    ofstream out(file_name, ios::binary | ios::app);


    schema s;
    err = s.load_schema(this->db_name, table_name);
    if (!err.ok()) {
        return err;
    }


    buffer b;
    err = b.verify(s, data, size_of_data);
    if (!err.ok()) {
        return err;
    }


    b.fill_buffer(s, data, size_of_data);

    out.write((char*)b.row_buffer, b.size);
    out.close();

    return err;
}

DB_error database::select_from_table(string table_name, ConditionNode* root, bool where) {
    string file_name = "..\\data\\" + this->db_name + '_' + table_name + ".tbl";

    DB_error err(ERR_NONE,"");

    if (!table_exists(table_name) && !schema_exists(table_name)) {
        return DB_error(ERR_UNKNOWN_TABLE,"table " + table_name + " does not exists");
    }

    ifstream out(file_name, ios::binary);
    schema s;
 
    err = s.load_schema(this->db_name, table_name);
    if(!err.ok()){
        return err;
    }
    
    buffer b;
    b.read_buffer(file_name);
   
    b.print_buffer(s,root,where);

    out.close();

    return err;
}

DB_error database :: delete_from_table(string table_name, ConditionNode* root, bool where){
    string file_name = "..\\data\\" + this->db_name + '_' + table_name + ".tbl";

    DB_error err(ERR_NONE,"");

    if (!table_exists(table_name) && !schema_exists(table_name)) {
        return DB_error(ERR_UNKNOWN_TABLE,"table " + table_name + " does not exists");
    }

    if(!where){
        ofstream out(file_name, ios::binary);
        out.close();
        return err;
    }

    schema s;
 
    err = s.load_schema(this->db_name, table_name);
    if(!err.ok()){
        return err;
    }
    
    buffer b;
    b.read_buffer(file_name);

    int updated_size = b.delete_from_buffer(s,root,where);

    ofstream out(file_name, ios::binary | ios::trunc);

    out.write((char*)b.row_buffer, updated_size);
    out.close();

    return err;
}

DB_error database::update_table(string table_name, ConditionNode* root, bool where,SetClause &sc){
    string file_name = "..\\data\\" + this->db_name + '_' + table_name + ".tbl";

    DB_error err(ERR_NONE,"");

    if (!table_exists(table_name) || !schema_exists(table_name)) {
        return DB_error(ERR_UNKNOWN_TABLE,"table " + table_name + " does not exists");
    }

    schema s;
    err = s.load_schema(this->db_name, table_name);
    if (!err.ok()) {
        return err;
    }

    
    buffer b;
    b.read_buffer(file_name);

    err = b.set_verify(s,sc);
    if (!err.ok()) {
        return err;
    }

    b.update_buffer(s,root,where,sc);
    ofstream out(file_name, ios::binary | ios::trunc);

    out.write((char*)b.row_buffer, b.size);
    out.close();

    return err;
}

DB_error database::drop_table(string table_name){
    string file_name = "..\\data\\" + this->db_name + '_' + table_name + ".tbl";
    string schema_name = "..\\data\\" + this->db_name + '_' + table_name + ".schema";
    DB_error err;

    if(remove(file_name.c_str())==0){
        //deleted
    }else{
        //unsuccesful
        return DB_error(ERR_RUNTIME,"table does not exists ");
    }


    if(remove(schema_name.c_str())==0){
        //deleted
    }else{
        //unsuccesful
        return DB_error(ERR_RUNTIME,"schema does not exists ");
    }


    catalog c;
    err = c.remove_table(table_name,this->db_name);
    if(!err.ok()){
        return err;
    }

    return DB_error(ERR_NONE,"");
}