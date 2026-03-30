#pragma once
#include <iostream>
#include <fstream>
// #include"utils.h"
using namespace std;
#define PAGE_SIZE 4096
#define MAX_KEY_SIZE 32

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
    void free_page(Table_Metadata &tmd, const int& pageId);
    void open_file(string &table_name);
    void create_file(string &table_name);
    void open_index(string &table_name, string &column_name);
    void create_index(string &table_name, string &column_name);

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