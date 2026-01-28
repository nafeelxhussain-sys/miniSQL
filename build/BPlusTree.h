#pragma once
#include <iostream>
#include <fstream>
// #include"utils.h"
using namespace std;
#define PAGE_SIZE 128
#define MAX_KEY_SIZE 32

// tbr
enum datatype
{
    int32 = 0,
    text = 1,
    bool8 = 2
};
// ||||||||||||||||||||||||||||||||||||||||||||||||||||

struct SplitInfo
{
    int right_page_id;
    char separator_key[MAX_KEY_SIZE];
};
class Table_Metadata
{
public:
    int leafnode_order;
    int internalnode_order;
    int total_page_count;
    int free_page_head;
    int root_page_id;
    int first_leaf_page_id;

    Table_Metadata();
    void load_table_md(void *buffer);
    void save_table_md(void *buffer);
};

struct Page
{
    char data[PAGE_SIZE];
};

class Disk_Manager
{
public:
    fstream file;

    ~Disk_Manager();
    void read_page(int page_id, void *buffer);
    void write_page(int page_id, const void *buffer);
    int allocate_page(Table_Metadata &tmd);
    void free_page(int pageId);
    void open_file(string &table_name);
    void create_file(string &table_name);

    void init_meta_page(void *buffer, const Table_Metadata &tmd);
    void init_meta_page(void *buffer, const int leadnode_order, const int internalnode_order);
    void init_internal_page(void *buffer, int pageid, int parentid = 0);
    void init_leaf_page(void *buffer, int pageid, int parentid = 0, int nextleaf = 0, int prevleaf = 0);
    void init_free_page(void *buffer, const int pageId, const int next_free);
};

enum pagetype
{
    LEAFNODE = 1,
    INTERNALNODE = 0,
    FREEPAGE = 2
};

class Header
{
public:
    pagetype page_type;
    int keys_count;
    int parent_id;
    int page_id;
    int free_bytes;

    int nextleaf;
    int prevleaf;

    void load_header(void *buffer);
    void save_header(void *buffer);
};

class BplusTree
{
    private:
    private:
    // Shared state for the current operation
    Disk_Manager &dm; 
    Table_Metadata &tmd;
    datatype dt;
    
    public:
    void create_tree(Disk_Manager &dm, int &row_size, int &key_size);

    int search_leaf(Disk_Manager &dm, Table_Metadata &tmd, datatype dt, const void *key, int key_size);

    int find_in_leaf(Disk_Manager &dm, const int &pageid, datatype dt, const void *key, const int &key_size, const int &row_size, const int &key_off);

    void insert_row(Disk_Manager &dm, datatype dt, const void *row, const int &key_size, const int &row_size, const int &key_off);

    SplitInfo leaf_split(Disk_Manager &dm, Table_Metadata &tmd, Page &left_pg, const int &row_size, const char *full, const int &key_size, const int &key_off);

    void insert_parent(Disk_Manager &dm, datatype dt, const int &parentId, const void *key, const int &key_size, const int &childpointer);
    
    void update_parent(Disk_Manager &dm, datatype dt, const int &parentId, const void *key, const int &key_size,const void *new_key);

    SplitInfo parent_split(Disk_Manager &dm, Table_Metadata &tmd, Page &left_pg, const void *key, const int &key_size, const int &child, const char *full);

    int find_in_parent(Disk_Manager &dm, datatype dt, const int &parentId, const void *key, const int &key_size);

    void create_new_root(Disk_Manager &dm, Table_Metadata &tmd, const int &leaf_id, const int &right_leaf_id, const void *seperator_key, const int &key_size);

    void delete_row(Disk_Manager &dm, datatype dt, const int &key_size, const int &row_size, const void *key_ptr, const int &key_off);

    void borrow_left(Disk_Manager &dm,datatype dt, const int &pageId,const int &key_size,const int &row_size, const int &key_off);

    void borrow_right(Disk_Manager &dm,datatype dt, const int &pageId,const int &key_size,const int &row_size, const int &key_off);

    void merge_leaf(Disk_Manager &dm,datatype dt, const int &pageId,const int &key_size,const int &row_size, const int &key_off);

    void delete_parent(Disk_Manager &dm,datatype dt, const int &pageId,const int &key_size,const char* seperator_key);

    void borrow_right_parent(Disk_Manager &dm,datatype dt, const int &pageId,const int &rightId,const int&rowId,const int &key_size);

    void borrow_left_parent(Disk_Manager &dm,datatype dt, const int &pageId,const int &leftId,const int&rowId,const int &key_size);
    
    void merge_parent(Disk_Manager &dm,datatype dt, const int &leftId,const int &RightId,const int &key_size);
    
    void delete_root(Disk_Manager &dm,Table_Metadata &tmd);

    // void validate_child(Disk_Manager &dm, int parent_id, int child_id, std::vector<int> &queue);
    // void check_integrity(Disk_Manager &dm, Table_Metadata &tmd,int key_size);
    // void print_tree_structure(Disk_Manager &dm, Table_Metadata &tmd, int key_size);
};
