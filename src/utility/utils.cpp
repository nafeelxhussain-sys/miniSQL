#include <iostream>
#include <string>
#include <cstring>
#include "utils.h"
using namespace std;

// ------------------- utility -------------------

int precedence(string op) {
    if (to_upper(op) == "AND") return 2;
    if (to_upper(op) == "OR") return 1;
    return 0;
}


string to_upper(string s) {
    for (int i = 0; i < s.size(); i++)
        s[i] = toupper(s[i]);
    return s;
}

string dtype_to_string(datatype dt) {
    switch(dt) {
        case int32: return "INT";
        case text:  return "TEXT";
        case bool8: return "BOOL";
        default:    return "UNKNOWN";
    }
}

int compare_composite(const void* buffer,  datatype dt, const void* search_key, int key_size){
    const char* page = (const char*)buffer;
    bool tie  =  false;
    key_size-=sizeof(int);

    if (dt == int32){
        int page_key;
        int search_val;

        memcpy(&search_val, search_key, sizeof(int));
        memcpy(&page_key, page , sizeof(int));

        if(search_val < page_key) return -1;
        else if(search_val > page_key) return 1;
        else if(search_val == page_key) tie=true;
    }
    else if (dt == text){
        int value =  memcmp(search_key, page , key_size);

        if(value<0) return -1;
        else if(value==0) tie=true;
        else if(value>0) return 1;
    }

    if(tie){
        int page_key;
        int search_val;

        char* pk_search = (char*)search_key +key_size;
        char* pk_buffer = (char*)buffer +key_size;

        memcpy(&search_val, pk_search, sizeof(int));
        memcpy(&page_key, pk_buffer , sizeof(int));


        if(search_val < page_key)  return -1;
        else if(search_val > page_key) return 1;
    }

    return 0;
}

int compare_keys(const void* buffer,datatype dt, const void* search_key,int key_size){
    const char* page = (const char*)buffer;

    if (dt == int32){
        int page_key;
        int search_val;

        memcpy(&search_val, search_key, sizeof(int));
        memcpy(&page_key, page , sizeof(int));

        if(search_val < page_key) return -1;
        else if(search_val > page_key) return 1;
        else if(search_val == page_key) return 0;
    }
    else if (dt == text){
        int value =  memcmp(search_key, page , key_size);

        if(value<0) return -1;
        else if(value==0) return 0;
        else if(value>0) return 1;
    }

    return 0;
}

string read_query() {
    string line, query;
    bool in_quote = false;

    while (true) {
        if (!getline(cin, line)) return "";  // EOF / Ctrl+D

        if(query.empty() && to_upper(line) == "EXIT"){
            return "EXIT";
        }

        if (line.empty() && !query.empty() && query.find(';') == string::npos) {
            if(query == " "){
                return "";
            }

            cout << "ERROR: missing ';'"<<endl<<endl;
            return "";
        }

        for (char c : line) {
            if (c == '\'') in_quote = !in_quote;
            query += c;
        }
        query += ' ';

        if (!in_quote && query.find(';') != string :: npos)
            break;
    }

    // remove trailing semicolon
    size_t pos = query.rfind(';');
    if (pos != string::npos)
        query.erase(pos, 1);

    return query;
}