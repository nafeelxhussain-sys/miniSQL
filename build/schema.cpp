#include<iostream>
#include<fstream>
#include<cstring>
#include"schema.h"
using namespace std;

// -------------------- schema --------------------
schema::schema() {
    col_offset = nullptr;
    column_name = nullptr;
    dtypes = nullptr;
    col_index=nullptr;
}

schema::~schema() {
    delete[] col_offset;
    delete[] column_name;
    delete[] dtypes;
    delete[] col_index;
}

void schema::free_arrays() {
    delete[] col_offset;
    delete[] column_name;
    delete[] dtypes;
    delete[] col_index;

    col_offset = nullptr;
    column_name = nullptr;
    dtypes = nullptr;
    col_index=nullptr;
}

DB_error schema::load_schema(string db_name, string table_name) {
    string file_name = "..\\data\\" + db_name + '_' + table_name + ".schema";
    ifstream in(file_name, ios::binary);
    unsigned char buffer[512] = {'\0'};

    if (!in.is_open()) {
        return DB_error(ERR_RUNTIME,"table " + table_name + " does not exists " );
    }

    in.seekg(0, ios::end);
    size_t size = in.tellg();
    in.seekg(0, ios::beg);

    in.read((char*)buffer, size);

    this->table_name = table_name;

    uint8_t temp8;
    uint16_t temp16;
    uint32_t temp32;

    memcpy(&temp8, buffer, 1);
    num_of_cols = temp8;

    free_arrays();

    col_offset = new int[num_of_cols];
    col_index = new int[num_of_cols];
    column_name = new string[num_of_cols];
    dtypes = new datatype[num_of_cols];

    memcpy(&is_clustered,buffer+1,1);

    memcpy(&temp16, buffer + 2, 2);
    row_size = temp16;

    memcpy(&temp16, buffer + 4, 2);
    page_size = temp16;

    int k = 0;
    int i = 7 + table_name.size();
    col_offset[0] = 0;
    while (k < num_of_cols) {
        memcpy(&temp8, buffer + i, 1);
        i++;

        string name(reinterpret_cast<const char*>(buffer + i), temp8);
        column_name[k] = name;
        i += temp8;

        memcpy(&temp8, buffer + i, 1);
        dtypes[k] = (datatype)temp8;
        i++;

        memcpy(&temp8, buffer + i, 1);
        col_index[k] = temp8;
        i++;

        if (k + 1 < num_of_cols) {
            memcpy(&temp16, buffer + i, 2);
            col_offset[k + 1] = temp16 + col_offset[k];
            i += 2;
        }

        k++;
    }

    return DB_error(ERR_NONE,"");
}

DB_error schema::create_schema_file(string db_name, string table_name, uint8_t num_of_cols, string* name, int* size, datatype* type,int* col_index, uint8_t clustered) {
    string file_name = "..\\data\\" + db_name + '_' + table_name + ".schema";
    ofstream out(file_name, ios::binary | ios::trunc);
    unsigned char buffer[512] = {'\0'};

    if (!out.is_open()) {
        return DB_error(ERR_UNKNOWN_TABLE,"table "+table_name + " does not exists");
    }

    memcpy(buffer, &num_of_cols, 1);

    memcpy(buffer+1,&clustered,1);

    uint16_t row_size = 0;
    for (int i = 0; i < num_of_cols; i++) row_size += size[i];
    memcpy(buffer + 2, &row_size, 2);

    uint16_t page_size = 4096;
    memcpy(buffer + 4, &page_size, 2);

    uint8_t tb_name_len = table_name.length();
    memcpy(buffer + 6, &tb_name_len, 1);

    memcpy(buffer + 7, table_name.c_str(), table_name.length());

    int i = 7 + table_name.length();
    for (int k = 0; k < num_of_cols; k++) {
        const uint8_t len = name[k].length();
        memcpy(buffer + i, &len, 1);
        memcpy(buffer + i + 1, name[k].c_str(), len);
        i += len + 1;

        memcpy(buffer + i, &type[k], 1);
        i += 1;

        memcpy(buffer+i,&col_index[k],1);
        i += 1;

        memcpy(buffer + i, &size[k], 2);
        i += 2;
    }

    out.write((char*)buffer, i);
    out.close();

    return DB_error(ERR_NONE,"");
}

void schema::print_schema() {
    string type = (this->is_clustered==1) ? "[clustered]" : "[heap]" ;
    string idx="";
    
    cout << table_name << "  " << type<< endl;
    cout << "|"<< endl;


    for (int i = 0; i < num_of_cols -1; i++) {
        if(col_index[i]==1)idx="PRIMARY KEY";
        else if(col_index[i]==2)idx="SECONDARY KEY";
        else if(col_index[i]==0)idx="";

        cout<<"|---- ";
        cout << column_name[i] << "    " << dtype_to_string(dtypes[i]) << " ( " << col_offset[i+1] - col_offset[i] << " ) " << idx<< endl;
    }

    int i = num_of_cols-1;
    if(col_index[i]==1)idx="PRIMARY KEY";
    else if(col_index[i]==2)idx="SECONDARY KEY";
    else if(col_index[i]==0)idx="";
    cout<<"|---- ";
    cout << column_name[i] << "    " << dtype_to_string(dtypes[i]) << " ( " << row_size - col_offset[i] << " ) " << idx <<endl;
}

int schema::getColumnOffset(int colIndex) { return col_offset[colIndex]; }

int schema::getColumnSize(int colIndex) {
    if (colIndex + 1 == num_of_cols) return row_size - col_offset[colIndex];
    return col_offset[colIndex + 1] - col_offset[colIndex];
}

datatype schema::getColumnType(int colIndex) { return dtypes[colIndex]; }

int schema::getColumnIndex(string column_name_){
    for(int i = 0 ; i<num_of_cols ; i++){
        if(to_upper(column_name_) == to_upper(column_name[i])){
            return i;
        }
    }

    return -1;
}

int schema::getColumnIndexType(string column_name_){
    for(int i = 0 ; i<num_of_cols ; i++){
        if(to_upper(column_name_) == to_upper(column_name[i])){
            return col_index[i];
        }
    }

    return -1;
}