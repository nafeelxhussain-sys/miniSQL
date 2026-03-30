# pragma once
#include<iostream>
#include"DiskManager.h"
#include"BPlusTree.h"
using namespace std;


class Index{
    public:
    BplusTree tree;

    int index_size;
    int pk_size;
    
    public:
    int search_lower_bound(const void* target_value, datatype dt);
    int search_upper_bound(const void* target_value, datatype dt);

    public:
    Index(Disk_Manager &dm, Table_Metadata &tmd, datatype dt, int pk_size, int index_size);
    void create_index();
    void open_index();
    void insert_index(const void* row);
    void update_index(const void* old_, const void* new_);
    void delete_index(const void* row);
    void find_all_pks(const void* target_value,datatype dt,function<void(const char*)> callback);
    void find_all_pks_forward(const void* target_value, datatype dt,function<void(const char*)> callback);
    void find_all_pks_backward(const void* target_value, datatype dt,function<void(const char*)> callback);

    void display_tree();
    void print_tree_recursive(int pid, int level);
    void print_level_order();
};


