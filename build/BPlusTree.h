#pragma once
#include <iostream>
#include <fstream>
#include "DiskManager.h"
#include "utils.h"
using namespace std;
#define PAGE_SIZE 4096



class BplusTree
{
    public:
    // Shared state for the current operation
    Disk_Manager &dm; 
    Table_Metadata &tmd;
    datatype dt;
    const int key_size;
    bool is_primary ; 
    

    //internal tree balancing functions
    protected:
    void sync_metadata();
    int search_leaf(const void *key);
    int find_in_leaf(const int &pageid, const void *key, const int &row_size, const int &key_off);
    SplitInfo leaf_split(Page &left_pg, const int &row_size, const char *full, const int &key_off);
    void insert_parent( const int &parentId, const void *key, const int &childpointer);
    void update_parent( const int &parentId, const void *key,const void *new_key);
    SplitInfo parent_split(Page &left_pg, const void *key,  const int &child, const char *full);
    int find_in_parent(const int &parentId, const void *key);
    void create_new_root(const int &leaf_id, const int &right_leaf_id, const void *seperator_key);
    void borrow_left(const int &pageId,const int &row_size, const int &key_off);
    void borrow_right(const int &pageId,const int &row_size, const int &key_off);
    void merge_leaf(const int &pageId,const int &row_size, const int &key_off);
    void delete_parent(const int &pageId,const char* seperator_key);
    void borrow_right_parent(const int &pageId,const int &rightId,const int&rowId);
    void borrow_left_parent(const int &pageId,const int &leftId,const int&rowId);
    void merge_parent(const int &leftId,const int &RightId);
    void delete_root();


    public:
    BplusTree(Disk_Manager &dm_ref, Table_Metadata &tmd_ref, datatype type, int k_size, bool is_primary);

    void create_tree(int &row_size);

    void open_tree();

    void insert_row(const void *row, const int &row_size, const int &key_off);

    void update_row(const void *key, const int &row_size,const int &key_off, const int &col_off, const int &col_size, const char* updated_value);

    void delete_row( const int &row_size, const void *key_ptr, const int &key_off);

    void scan_all(const int &row_size, function<void(const char*)> callback);

    void scan_forward(const int &row_size,const char* key,const int &key_off,function<void(const char*)> callback);

    void scan_backward(const int &row_size, const char* key,const int &key_off,function<void(const char*)> callback);
    
    void scan_point(const int &row_size, const char* key,const int &key_off,function<void(const char*)> callback);
};
