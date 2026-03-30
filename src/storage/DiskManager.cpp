#include<iostream>
#include<cstring>
#include<bits/stdc++.h>
#include"DiskManager.h"
// #include "BPlusTree.h"
// #include "utils.h"
using namespace std;

void Disk_Manager :: open_file(string &table_name){
    string file_name = "..//data//main_" + table_name + ".tbl";

    file.open(file_name, ios::in | ios::out | ios::binary);

    if (!file.is_open()) {
        //error
    }
}

void Disk_Manager:: create_file(string &table_name){
    string file_name = "..//data//main_" + table_name + ".tbl";

    if (file.is_open()) {
        file.close();
    }

    // create file
    file.open(file_name, ios::out | ios::binary);
    file.close();

    // reopen
    file.open(file_name, ios::in | ios::out | ios::binary);

    if (!file.is_open()) {
        //error
    }

}

void Disk_Manager :: open_index(string &table_name, string &column_name){
    string file_name = "..//data//main_" + table_name + '_' + column_name +".tbl";

    file.open(file_name, ios::in | ios::out | ios::binary);

    if (!file.is_open()) {
        //error
    }
}

void Disk_Manager:: create_index(string &table_name, string &column_name){
    string file_name = "..//data//main_" + table_name + '_' + column_name +".tbl";

    if (file.is_open()) {
        file.close();
    }

    // create file
    file.open(file_name, ios::out | ios::binary);
    file.close();

    // reopen
    file.open(file_name, ios::in | ios::out | ios::binary);

    if (!file.is_open()) {
        //error
    }

}

Disk_Manager:: ~Disk_Manager(){
    if(file.is_open()){
        file.close();
    }
}

void Disk_Manager :: read_page(int page_id, void *buffer){
    memset(buffer, 0, PAGE_SIZE);
    int page_offset = page_id * PAGE_SIZE;
    file.seekg(page_offset);
    file.read((char*)buffer, PAGE_SIZE);
}

void Disk_Manager :: write_page(int page_id,const void *buffer){
    int page_offset = page_id * PAGE_SIZE;
    file.seekp(page_offset);
    file.write((char*)buffer, PAGE_SIZE);
}

int Disk_Manager :: allocate_page(Table_Metadata &tmd){
    int page_id = 0;

    //free page
    if(tmd.free_page_head != 0){
        page_id = tmd.free_page_head;
        
        Page buffer;
        Header h;
        
        read_page(page_id,&buffer);
        h.load_header(&buffer);
        
        //move the free page pointer to next 
        tmd.free_page_head = h.nextleaf;
        
    }else{
        //no free page
        tmd.total_page_count++;
        page_id = tmd.total_page_count;
    }

    if(page_id==1){
        tmd.first_leaf_page_id = page_id;
        tmd.root_page_id = page_id;
    }

    Page meta;
    init_meta_page(meta.data, tmd);
    write_page(0,meta.data);

    return page_id;
}

void Disk_Manager :: free_page(Table_Metadata &tmd,const int &pageId){
    Page buffer;
    Page meta;

    init_free_page(&buffer,pageId,tmd.free_page_head);
    tmd.free_page_head = pageId;

    tmd.save_table_md(&meta);

    write_page(0,&meta);
    write_page(pageId, &buffer);
}

void Disk_Manager :: init_meta_page(void *buffer, const Table_Metadata &tmd){
    memset(buffer,0,PAGE_SIZE);

    char* index  = (char*)buffer;
    memcpy(index, &tmd.total_page_count,4);
    index+=4;

    memcpy(index, &tmd.root_page_id,4);
    index+=4;

    memcpy(index, &tmd.first_leaf_page_id,4);
    index+=4;

    memcpy(index, &tmd.free_page_head,4);
    index+=4;

    memcpy(index, &tmd.leafnode_order,4);
    index+=4;

    memcpy(index, &tmd.internalnode_order,4);
}

void Disk_Manager::init_free_page(void *buffer, const int pageId,const int next_free){
    memset(buffer, 0, PAGE_SIZE);
    char* index  = (char*)buffer;

    pagetype pt = FREEPAGE;

    memcpy(index,&pt,4);index+=4;
    memcpy(index,&pageId,4);index+=4;
    memcpy(index,&next_free,4);
}

void Disk_Manager::init_meta_page(void *buffer, const int leafnode_order, const int internalnode_order) {
    memset(buffer, 0, PAGE_SIZE);

    char* index = (char*)buffer;

    int total_page_count = 0;
    int root_page_id = 0;
    int first_leaf_page_id = 0;
    int free_page_head = 0;

    memcpy(index, &total_page_count, 4); index += 4;
    memcpy(index, &root_page_id, 4); index += 4;
    memcpy(index, &first_leaf_page_id, 4); index += 4;
    memcpy(index, &free_page_head, 4); index += 4;

    memcpy(index, &leafnode_order, 4); index += 4;
    memcpy(index, &internalnode_order, 4);
}

void Disk_Manager::init_internal_page(void *buffer, int pageid, int parentid){
    memset(buffer, 0, PAGE_SIZE);
    char* index = (char*) buffer;

    pagetype type = INTERNALNODE;
    int key_count = 0;
    int next = 0;
    int prev = 0;
    int free_bytes = PAGE_SIZE - sizeof(Header);

    memcpy(index,&type,4); index+=4;
    memcpy(index,&pageid,4); index+=4;
    memcpy(index,&parentid,4); index+=4;
    memcpy(index,&key_count,4); index+=4;
    memcpy(index,&free_bytes,4); index+=4;
    memcpy(index,&next,4); index+=4;
    memcpy(index,&prev,4); index+=4;
}

void Disk_Manager::init_leaf_page(void *buffer, int pageid, int parentid,int nextleaf, int prevleaf){
    memset(buffer, 0, PAGE_SIZE);
    char* index = (char*) buffer;

    pagetype type = LEAFNODE;
    int key_count = 0;
    int free_bytes = PAGE_SIZE - sizeof(Header);

    memcpy(index,&type,4); index+=4;
    memcpy(index,&pageid,4); index+=4;
    memcpy(index,&parentid,4); index+=4;
    memcpy(index,&key_count,4); index+=4;
    memcpy(index,&free_bytes,4); index+=4;
    memcpy(index,&nextleaf,4); index+=4;
    memcpy(index,&prevleaf,4); index+=4;
}

void Table_Metadata::load_table_md(void *buffer){
    char* index = (char*) buffer;

    memcpy(&total_page_count,index,4); index+=4;
    memcpy(&root_page_id,index,4); index+=4;
    memcpy(&first_leaf_page_id,index,4); index+=4;
    memcpy(&free_page_head,index,4); index+=4;
    memcpy(&leafnode_order,index,4); index+=4;
    memcpy(&internalnode_order,index,4); index+=4;
}

void Table_Metadata::save_table_md(void *buffer){
    memset(buffer, 0, PAGE_SIZE);
    char* index = (char*) buffer;

    memcpy(index,&total_page_count,4); index+=4;
    memcpy(index,&root_page_id,4); index+=4;
    memcpy(index,&first_leaf_page_id,4); index+=4;
    memcpy(index,&free_page_head,4); index+=4;
    memcpy(index,&leafnode_order,4); index+=4;
    memcpy(index,&internalnode_order,4); index+=4;
}

void Header::load_header(void *buffer){
    char* index = (char*) buffer;

    memcpy(&page_type,index,4); index+=4;
    memcpy(&page_id,index,4); index+=4;

    if(page_type == FREEPAGE){
        memcpy(&nextleaf,index,4); return;
    }
    
    memcpy(&parent_id,index,4); index+=4;
    memcpy(&keys_count,index,4); index+=4;
    memcpy(&free_bytes,index,4); index+=4;
    
    if(page_type==LEAFNODE){
        memcpy(&nextleaf,index,4); index+=4;
        memcpy(&prevleaf,index,4); index+=4;
    }else{
        nextleaf = 0;
        prevleaf = 0;
    }
}

void Header::save_header(void *buffer){
    char* index = (char*) buffer;

    memcpy(index,&page_type,4); index+=4;
    memcpy(index,&page_id,4); index+=4;
    memcpy(index,&parent_id,4); index+=4;
    memcpy(index,&keys_count,4); index+=4;
    memcpy(index,&free_bytes,4); index+=4;
    memcpy(index,&nextleaf,4); index+=4;
    memcpy(index,&prevleaf,4); index+=4;
}

Table_Metadata::Table_Metadata(){
    leafnode_order=0;
    internalnode_order=0;
    total_page_count=0;
    free_page_head=0;
    root_page_id=0;
    first_leaf_page_id=0;
}