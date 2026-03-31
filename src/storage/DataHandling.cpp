#include<iostream>
#include<cstring>
#include<vector>
#include<iomanip>
#include"DataHandling.h"
#include"error.h"
#include"schema.h"
#include"filter.h"
using namespace std;

DB_error DataHandler::set_verify(schema &s, SetClause &sc){
    for (int i = 0; i < sc.set_cols; i++) {
        int index = s.getColumnIndex(sc.column_names[i]);
        if (index == -1)
            return DB_error(ERR_UNKNOWN_COLUMN, "unknown column: " + sc.column_names[i]);
        
        int index_type = s.getColumnIndexType(sc.column_names[i]);
        if(index_type == 1)
            return DB_error(ERR_RUNTIME, "PRIMARY KEY is immutable: ");

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

DB_error DataHandler::data_verify(schema& s, string* data, int size_of_data) {
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
                    size_t pos;
                    stoi(test, &pos);
                    if (pos != test.size())
                        return DB_error(ERR_TYPE_MISMATCH, "expected INT: " + test);
                }
                catch (...) {
                    return DB_error(ERR_TYPE_MISMATCH,"expected INT: " + data[index]);
                }
                break;
            case 1: // text
                text_size = (c + 1 == s.num_of_cols) ? s.row_size - s.col_offset[c] :
                    s.col_offset[c + 1] - s.col_offset[c];
                if (test.size() > text_size) return DB_error(ERR_RUNTIME,"Data entered is too long: " + data[index]);
                break;
            case 2: // bool
                if (test.empty()) return DB_error(ERR_TYPE_MISMATCH,"expected BOOL: " +data[index]);
                if (to_upper(test) != "TRUE" && to_upper(test) != "FALSE")  return DB_error(ERR_TYPE_MISMATCH,"expected BOOL: " +data[index]);
            }
        }
    }

    return DB_error(ERR_NONE,"");
}

vector<int> DataHandler::get_col_widths(schema& s) {
    std::vector<int> widths;
    for (int i = 0; i < s.num_of_cols; i++) {
        int col_name_len = s.column_name[i].length();
        datatype dt = s.getColumnType(i);
        
        int data_len = (dt == int32) ? 11 : (dt == bool8 ? 5 : s.getColumnSize(i));
        
        widths.push_back(max(col_name_len, data_len));
    }
    return widths;
}
void DataHandler::print_table_border(const std::vector<int>& widths) {
    cout << "+";
    for (int w : widths) {
        cout << string(w + 2, '-') << "+"; // +2 for internal padding
    }
    cout << endl;
}

void DataHandler::print_header(schema& s, const std::vector<int>& widths) {
    print_table_border(widths);
    cout << "| ";
    for (int i = 0; i < s.num_of_cols; i++) {
        
        cout << left << setw(widths[i]) << s.column_name[i] << " | ";
    }
    cout << endl;
    print_table_border(widths);
}

void DataHandler::print_row(schema& s, const char* row, const vector<int>& widths) {
    cout << "| ";
    for (int c = 0; c < s.num_of_cols; c++) {
        datatype dt = s.getColumnType(c);
        const char* start = row + s.col_offset[c];
        int w = widths[c];

        if (dt == int32) {
            int32_t val;
            memcpy(&val, start, 4);
            cout << right << setw(w) << val;
        } 
        else if (dt == text) {
            std::string val(start, s.getColumnSize(c));
            size_t end = val.find('\0');
            if (end != string::npos) val = val.substr(0, end);
            cout << left << setw(w) << val;
        } 
        else if (dt == bool8) {
            uint8_t val;
            memcpy(&val, start, 1);
            std::string truth = (val == 1) ? "True" : "False";
            cout << left << setw(w) << truth;
        }
        cout << " | ";
    }
    cout << endl;
}

void DataHandler::converter(schema& s, string *data, char* row){
    //converts into row (raw buffer)
    char* row_ptr = (char*) row;

    for(int i = 0 ; i<s.num_of_cols ; i++ ){
        datatype dt = s.getColumnType(i);

        if (dt == int32) {
            int x = stoi(data[i]);
            memcpy(row_ptr, &x, sizeof(int));
            row_ptr += sizeof(int);
        }

        else if (dt == bool8) {
            uint8_t x = 0;
            if (data[i] == "1" || to_upper(data[i]) == "TRUE") x = 1;
            memcpy(row_ptr, &x, sizeof(uint8_t));
            row_ptr += sizeof(uint8_t);
        }

        else if (dt == text) {
            int col_size = s.getColumnSize(i);
            memcpy(row_ptr, data[i].c_str(), data[i].size());
            memset(row_ptr + data[i].size(), 0, col_size - data[i].size());
            row_ptr += col_size;
        }
    }
}

void DataHandler::converter(char* index_row, char* index_ptr, char* key_ptr, int index_size, int key_size){
    char* ptr = (char*)index_row;
    memcpy(ptr, index_ptr,index_size);
    ptr += index_size;
    memcpy(ptr, key_ptr,key_size);
}

void DataHandler::converter(char* ptr, string value, datatype dt, int size){
    if(dt==int32){
        int x = stoi(value);
        memcpy(ptr, &x, sizeof(int));
    }
    else if(dt == bool8){
        uint8_t x = 0;
        if(to_upper(value)=="TRUE")
        x = 1;
        memcpy(ptr, &x, sizeof(uint8_t));
    }
    else{
        memcpy(ptr, value.c_str(), value.size());
        memset(ptr + value.size() , 0 , size - value.size());
    }
}