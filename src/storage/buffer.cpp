#include<iostream>
#include<fstream>
#include <cstring>
#include"buffer.h"
#include"schema.h"
#include"filter.h"
using namespace std;


// -------------------- buffer --------------------
buffer::buffer() {
    this->size = 4096;
    row_buffer = new unsigned char[size];
}

DB_error buffer::set_verify(schema &s, SetClause &sc){
    for (int i = 0; i < sc.set_cols; i++) {

        int index = s.getColumnIndex(sc.column_names[i]);
        if (index == -1)
            return DB_error(ERR_UNKNOWN_COLUMN, "unknown column: " + sc.column_names[i]);

        datatype dt = s.getColumnType(index);
        const string& test = sc.column_values[i];

        switch (dt) {
        case int32: {
            try {
                size_t pos;
                stoi(test, &pos);
                if (pos != test.size())
                    return DB_error(ERR_TYPE_MISMATCH, "expected INT: " + test);
            }
            catch (...) {
                return DB_error(ERR_TYPE_MISMATCH, "expected INT: " + test);
            }
            break;
        }

        case text: {
            int text_size =
                (index + 1 == s.num_of_cols) ?
                s.row_size - s.col_offset[index] :
                s.col_offset[index + 1] - s.col_offset[index];

            if ((int)test.size() > text_size)
                return DB_error(ERR_RUNTIME, "data too long: " + test);
            break;
        }

        case bool8: {
            if (test.empty())
                return DB_error(ERR_TYPE_MISMATCH, "expected BOOL");

            string v = to_upper(test);
            if (v != "TRUE" && v != "FALSE")
                return DB_error(ERR_TYPE_MISMATCH, "expected BOOL");

            break;
        }
        }
    }

    return DB_error(ERR_NONE, "");
}


DB_error buffer::verify(schema& s, string* data, int size_of_data) {
    if (size_of_data % s.num_of_cols != 0) return DB_error(ERR_UNKNOWN_COLUMN,"invalid column ");

    int rows = size_of_data / s.num_of_cols;

    for (int r = 0; r < rows; r++) {
        for (int c = 0; c < s.num_of_cols; c++) {
            int index = r * s.num_of_cols + c;
            string test = data[index];
            int text_size = 0;

            switch (s.dtypes[c]) {
            case 0: // int32
                try {
                    int value = stoi(test);
                }
                catch (...) {
                    return DB_error(ERR_TYPE_MISMATCH,"expected INT ");
                }
                break;
            case 1: // text
                text_size = (c + 1 == s.num_of_cols) ? s.row_size - s.col_offset[c] :
                    s.col_offset[c + 1] - s.col_offset[c];
                if (test.size() > text_size) return DB_error(ERR_RUNTIME,"Data entered is too long : " + data[index]);
                break;
            case 2: // bool
                if (to_upper(test) != "TRUE" && to_upper(test) != "FALSE")  return DB_error(ERR_TYPE_MISMATCH,"expected BOOL ");
                if (test.empty()) return DB_error(ERR_TYPE_MISMATCH,"expected BOOL ");
            }
        }
    }

    return DB_error(ERR_NONE,"");
}

void buffer::convert_and_write(const string& value, datatype dt, int position, int limit) {
    if (dt == int32) {
        uint32_t x = stoi(value);
        memcpy(row_buffer + position, &x, 4);
        return;
    }

    if (dt == bool8) {
        uint8_t x = 0;
        if (value == "1" || to_upper(value) == "TRUE") x = 1;
        memcpy(row_buffer + position, &x, 1);
        return;
    }

    if (dt == text) {
        memcpy(row_buffer + position, value.c_str(), value.size());
        memset(row_buffer + position + value.size(), 0, limit - value.size());
        return;
    }
}

void buffer::fill_buffer(schema& s, string* data, int size_of_data) {
    int current_row = 0;
    int total_rows = size_of_data / s.num_of_cols;
    this->size = s.row_size * total_rows;

    while (current_row < total_rows) {
        int current_row_start = current_row * s.row_size;
        int current_col = 0;

        while (current_col < s.num_of_cols) {
            int current_col_start = current_row_start + s.col_offset[current_col];
            int index = current_row * s.num_of_cols + current_col;
            int limit = (current_col + 1 == s.num_of_cols) ? s.row_size - s.col_offset[current_col] :
                s.col_offset[current_col + 1] - s.col_offset[current_col];

            convert_and_write(data[index], s.dtypes[current_col], current_col_start, limit);

            current_col++;
        }

        current_row++;
    }
}

void buffer::read_buffer(string path) {
    ifstream out(path, ios::binary);

    out.seekg(0, ios::end);
    size_t s = out.tellg();
    out.seekg(0, ios::beg);

    out.read((char*)row_buffer, s);
    this->size = s;
}

void buffer::print_buffer(schema& s, ConditionNode *root,bool is_where) {
    int rows = size / s.row_size;

    for (int i = 0; i < rows; i++) {
        const unsigned char* current_row_start = row_buffer + (i * s.row_size);

        //where clause
        bool condition_bool = true; 
        if(is_where){
        try{
            where_clause w;
            condition_bool = w.evaluvate_tree(root,s,current_row_start);
        }
        catch(const DB_error& err){
            cout<<err.message<<endl;
            return;
        }}

        if(!condition_bool){
            continue;
        }

        for (int k = 0; k < s.num_of_cols; k++) {
            datatype type = s.dtypes[k];
            const unsigned char* col_data_ptr = current_row_start + s.col_offset[k];

            switch (type) {
            case int32:
            {
                int32_t x;
                memcpy(&x, col_data_ptr, 4);
                cout << x << " ";
            } break;
            case text:
            {
                int text_length = (k + 1 == s.num_of_cols) ? s.row_size - s.col_offset[k] :
                s.col_offset[k + 1] - s.col_offset[k];
                for (int x = 0; x < text_length; x++) cout << col_data_ptr[x];
                cout << " ";
            } break;
            case bool8:
                if ((col_data_ptr[0] & 1) == 1) cout << "True" << " ";
                else cout << "False" << " ";
                break;
            }
        }
        cout << endl;
    }
}

void buffer :: update_buffer(schema& s, ConditionNode *root,bool is_where, SetClause &sc){
    int rows = size / s.row_size;

    where_clause w;

    for (int i = 0; i < rows; i++) {
        unsigned char* current_row_start = row_buffer + (i * s.row_size);

        //where clause
        bool condition_bool = true; 
        if(is_where){
        try{
            condition_bool = w.evaluvate_tree(root,s,current_row_start);
        }
        catch(const DB_error& err){
            cout<<err.message<<endl;
            return;
        }}
        


        if(!condition_bool){
            continue;
        }

        for (int k = 0; k < s.num_of_cols; k++) {

            //set clause
            int index = sc.find_column(s.column_name[k]);
            if(index==-1)continue;


            datatype type = s.dtypes[k];
            unsigned char* col_data_ptr = current_row_start + s.col_offset[k];

            switch (type) {
            case int32:
            {
                uint32_t x = (uint32_t)stoul(sc.column_values[index]);
                memcpy((char*)col_data_ptr, &x, sizeof(uint32_t));
                break;
            } 
            case text:
            {
                int text_length = (k + 1 == s.num_of_cols) ? s.row_size - s.col_offset[k] :
                s.col_offset[k + 1] - s.col_offset[k];
                int updated_len = sc.column_values[index].size();

                memcpy((char*)col_data_ptr,sc.column_values[index].c_str(), updated_len);
                memset((char*)(col_data_ptr+updated_len),'\0',text_length- updated_len);
                break;
            } 
            case bool8:
                uint8_t x=0;
                if(to_upper(sc.column_values[index]) == "TRUE") x = 1;
                else if(to_upper(sc.column_values[index]) == "FALSE") x =0;

                memcpy((char*)col_data_ptr, &x,sizeof(uint8_t) );
            }
        }
        
    }
}

int buffer::delete_from_buffer(schema& s, ConditionNode* root, bool is_where){
    int rows = size / s.row_size;

    int write_offset = 0;

    for (int i = 0; i < rows; i++) {

        unsigned char* current = row_buffer + i * s.row_size;

        bool condition_bool = false;

        if (is_where) {
            try {
                where_clause w;
                condition_bool = w.evaluvate_tree(root, s, current);
            }
            catch (const DB_error& err) {
                cout << err.message << endl;
                return 0;
            }
        }

        if (condition_bool) {
            // delete row → skip it
            continue;
        }

        // keep row → move it forward if needed
        if (write_offset != i * s.row_size) {
            memmove(row_buffer + write_offset, current, s.row_size);
        }

        write_offset += s.row_size;
    }

    return write_offset;  // new file size
}
