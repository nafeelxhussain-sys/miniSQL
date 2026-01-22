#include<iostream>
#include<cstring>
#include<bits/stdc++.h>
#include "BPlusTree.h"
// #include "utils.h"
using namespace std;

//tb removed


int compare_keys(const void* buffer,  datatype dt, const void* search_key, int key_size){
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

    return -1;
}
// |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
void Disk_Manager:: open_file(string &table_name){
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
    file.flush();
}

int Disk_Manager :: allocate_page(Table_Metadata &tmd){
    int page_id = 0;

    //free page
    if(tmd.free_page_head != 0){
        //move the free page pointer to next 
        //tbu

        page_id = tmd.free_page_head;
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


void BplusTree::create_tree(Disk_Manager &dm,int &row_size, int &key_size){
    //create meta data
    Table_Metadata tmd;
    
    tmd.internalnode_order = (PAGE_SIZE-sizeof(Header)-4)/(key_size+4);
    tmd.leafnode_order = (PAGE_SIZE-sizeof(Header))/row_size;

    Page buffer;

    //create and write the meta page
    tmd.save_table_md(&buffer);
    dm.write_page(0,&buffer);

    //allocate pageid for root
    int pid = dm.allocate_page(tmd);

    //create root
    dm.init_leaf_page(&buffer,pid);
    dm.write_page(pid,&buffer);
}

int BplusTree::search_leaf(Disk_Manager &dm, Table_Metadata &tmd, datatype dt,const void *key, int key_size){
    Page buffer;
    Header h;

    int current_pid = tmd.root_page_id;
    pagetype current_pagetype;

    char* data = (char*)&buffer;
    int depth = 0;

    while(true){
        if(++depth > 32) return 0;

        dm.read_page(current_pid,&buffer);
        h.load_header(&buffer);

        current_pagetype = h.page_type;
        if(current_pagetype == LEAFNODE) break;

        int next_pid = 0;
        for(int i = 0 ; i < h.keys_count ; i++){
            int key_off = sizeof(Header) + sizeof(int) + i*(4+key_size);
            int child_off = sizeof(Header)+i*(4+key_size);

            if(compare_keys(data+key_off ,dt,key,key_size)<0){
                //returns -1 if search_k<key
                memcpy(&next_pid, data + child_off,sizeof(int));
                break;
            }
        }

        if(next_pid==0){
            int child_off = (sizeof(Header)+h.keys_count*(4+key_size));
            memcpy(&next_pid, data + child_off,4);
        }

        if(next_pid==0) return 0;

        current_pid = next_pid;
    }

    return current_pid;
}

int BplusTree::find_in_leaf(Disk_Manager &dm,const int &pageid ,datatype dt,const void *key,
    const int &key_size,const int &row_size, const int &key_off){
    // binary search on the rows to get the position to insert
    //- represents that the key actually already exists 
    //+ the place where if it exists it should be at 

    Page buffer;
    dm.read_page(pageid,&buffer);

    Header h;
    h.load_header(&buffer);

    int end = h.keys_count-1;
    int start = 0;
    int mid = (end-start)/2; 
    
    while(end >= start){
        mid =start +  (end-start)/2; 

        char* index = (char*)&buffer + row_size * mid + sizeof(Header) + key_off;

        int compared = compare_keys(index,dt,key,key_size);
        //returns 1 when key is smaller
        
        if(compared == 0){return -mid;}
        else if(compared==1){
            start = mid+1;
        }else{
            end = mid-1;
        }
    }
    return start;
}

void BplusTree::insert_leaf(Disk_Manager &dm,datatype dt, const void* row,
    const int &key_size,const int &row_size, const int &key_off){
    
    Table_Metadata tmd;
    Header h;
    Page pg;

    dm.read_page(0,&pg);
    tmd.load_table_md(&pg);
    const char* search_key = (char*)row + key_off;


    //find the page in which insertion is going to happen
    int leaf_id = search_leaf(dm,tmd,dt,search_key,key_size);

    dm.read_page(leaf_id,&pg);
    h.load_header(&pg);
    int parentId=h.parent_id;

    //find the rowId to insert
    int rId = find_in_leaf(dm,leaf_id,dt,search_key,key_size,row_size,key_off);

    if(h.free_bytes >= row_size){

        if(rId < 0){
            //the key already exists and we cannot create a copy of pk
            //code tbu
            return ;
        }

        //insert the row 
        char* base = (char*)&pg + sizeof(Header);
        void* src  = base + rId * row_size;
        void* dest = base + (rId + 1) * row_size;

        int bytes_to_move = h.keys_count*row_size - rId*row_size;

        memmove(dest , src , bytes_to_move);
        memcpy(src,row,row_size);


        //update the header
        h.keys_count++;
        h.free_bytes -= row_size;

        h.save_header(&pg);


        //write the page back
        dm.write_page(leaf_id,&pg);

        return ;
    }

    //insert the row in the buffer
    int total_bytes = row_size * (h.keys_count+1);
    char* full = new char[total_bytes];

    void *old_base = (char*)&pg + sizeof(Header);
    memcpy(full , old_base, h.keys_count*row_size);

    char* base = (char*)full;
    void* src  = base + rId * row_size;
    void* dest = base + (rId + 1) * row_size;

    int bytes_to_move = h.keys_count*row_size - rId*row_size;

    memmove(dest , src , bytes_to_move);
    memcpy(src,row,row_size);

    //leaf split case
    SplitInfo res = leaf_split(dm,tmd,pg, row_size,full,key_size,key_off);
    delete[] full;


    if(tmd.root_page_id == leaf_id){
        //create a new root
        create_new_root(dm,tmd,leaf_id,res.right_page_id,res.separator_key,key_size); 
        return ;
    }

    //update the parent node
    insert_parent(dm,dt,h.parent_id,res.separator_key,key_size,res.right_page_id);
}

void BplusTree::create_new_root(Disk_Manager &dm,Table_Metadata &tmd,const int &leaf_id,
    const int &right_leaf_id,const void *seperator_key, const int &key_size){

    Page buffer;
    Header h;
    int rootId = dm.allocate_page(tmd);

    dm.init_internal_page(&buffer,rootId);
    h.load_header(&buffer);
    tmd.root_page_id = rootId;

    //insert childs and keys
    char* index = (char*)&buffer + sizeof(Header);

    memcpy(index, &leaf_id, sizeof(int)); index+=sizeof(int);
    memcpy(index, seperator_key, key_size); index+=key_size;
    memcpy(index, &right_leaf_id, sizeof(int));index+=sizeof(int);
    

    //update header
    h.keys_count=1;
    h.free_bytes=PAGE_SIZE - sizeof(Header) - 2*sizeof(int) - key_size;
    
    h.save_header(&buffer);

    dm.write_page(rootId,&buffer);
    Page meta;
    tmd.save_table_md(&meta);
    dm.write_page(0,&meta);

    //update childs
    dm.read_page(leaf_id,&buffer);
    h.load_header(&buffer);
    h.parent_id=rootId;
    h.save_header(&buffer);
    dm.write_page(leaf_id,&buffer);

    dm.read_page(right_leaf_id,&buffer);
    h.load_header(&buffer);
    h.parent_id=rootId;
    h.save_header(&buffer);
    dm.write_page(right_leaf_id,&buffer);
    
}

int BplusTree::find_in_parent(Disk_Manager &dm,datatype dt,const int &parentId ,const void *key,const int &key_size){
    // binary search on the (key+child) to get the position to insert
    //- represents that the key actually already exists 
    //+ the place where if it exists it should be at 

    Page buffer;
    dm.read_page(parentId,&buffer);

    Header h;
    h.load_header(&buffer);

    int end = h.keys_count-1;
    int start = 0; 
    int pos_size = key_size+4;
    
    while(end >= start){
        int mid =start +  (end-start)/2; 

        char* index = (char*)&buffer + pos_size * mid + sizeof(Header) +4;

        int compared = compare_keys(index,dt,key,key_size);
        //returns 1 when key is smaller
        
        if(compared == 0){return -mid;}
        else if(compared==1){
            start = mid+1;
        }else{
            end = mid-1;
        }
    }
    return start;
}

void BplusTree::insert_parent(Disk_Manager &dm,datatype dt,const int &pageId ,
    const void *key,const int &key_size,const int &child){

    Table_Metadata tmd;
    Header h;
    Page pg;

    dm.read_page(0,&pg);
    tmd.load_table_md(&pg);
    dm.read_page(pageId,&pg);
   
    h.load_header(&pg);
    int parentId=h.parent_id;
    int row_size = key_size + sizeof(int);

    //find the rowId to insert
    int rowId = find_in_parent(dm,dt,pageId,key,key_size);

    if(h.free_bytes >= row_size){
        if(rowId < 0){
            //the key already exists and we cannot create a copy of pk
            //code tbu
            return ;
        }

        //insert the row 
        char* base = (char*)&pg + sizeof(Header) + sizeof(int);
        char* src  = base + rowId * row_size;
        char* dest = base + (rowId + 1) * row_size;

        int bytes_to_move = h.keys_count*row_size - rowId*row_size;

        memmove(dest , src , bytes_to_move);
        memcpy(src,key,key_size);
        memcpy(src+key_size,&child,sizeof(int));


        //update the header
        h.keys_count++;
        h.free_bytes -= row_size;

        h.save_header(&pg);


        //write the page back
        dm.write_page(pageId,&pg);

        return ;
    }

    //insert the row in the buffer
    int total_bytes = row_size * (h.keys_count+1) + sizeof(int);
    char* full = new char[total_bytes];

    void *old_base = (char*)&pg + sizeof(Header);
    memcpy(full , old_base, sizeof(int) + h.keys_count*row_size);

    char* base = (char*)full+sizeof(int);
    char* src  = base + rowId * row_size;
    char* dest = base + (rowId + 1) * row_size;

    int bytes_to_move = h.keys_count*row_size - rowId*row_size;

    memmove(dest , src , bytes_to_move);
    memcpy(src,key,key_size);src+=key_size;
    memcpy(src,&child,sizeof(int));

    
    //internal split case
    SplitInfo res = parent_split(dm,tmd,pg,key,key_size,child,full);
    delete[] full;
    

    if(tmd.root_page_id == pageId){
        //create a new root
        create_new_root(dm,tmd,pageId,res.right_page_id,res.separator_key,key_size); 
        return ;
    }

    //update the parent node
    insert_parent(dm,dt,h.parent_id,res.separator_key,key_size,res.right_page_id);
}


SplitInfo BplusTree::parent_split(Disk_Manager &dm,Table_Metadata &tmd,Page &left_pg,const void * key,const int &key_size,
    const int &child,const char* full){

    Header h_left;
    Header h_right;

    h_left.load_header(&left_pg);
    int row_size = key_size + sizeof(int);
    
    Page right_pg;
    int right_pageId = dm.allocate_page(tmd);
    
    dm.init_internal_page(&right_pg,right_pageId,h_left.parent_id);
    h_right.load_header(&right_pg);

    int total_rows  = h_left.keys_count+1;
    int rows_to_shift = total_rows/2;
    int rows_unshifted = total_rows - rows_to_shift-1;

    //shift the rows
    char* dest_left = (char*)&left_pg+sizeof(Header);
    char* dest_right = (char*)&right_pg+sizeof(Header);
    char* src_left = (char*)full;
    char* src_right = (char*)full + (rows_unshifted+1)*row_size;

    int left_bytes = rows_unshifted*row_size+sizeof(int);
    int right_bytes = (rows_to_shift+1)*row_size;

    memcpy(dest_left ,src_left ,left_bytes );
    memset(dest_left + left_bytes, 0, PAGE_SIZE - sizeof(Header) - left_bytes);

    memcpy(dest_right,src_right,right_bytes);
    memset(dest_right + right_bytes, 0, PAGE_SIZE - sizeof(Header) - right_bytes);

    //update key count
    h_left.keys_count = rows_unshifted;
    h_right.keys_count = rows_to_shift;


    //update the free bytes
    h_left.free_bytes = PAGE_SIZE - sizeof(Header) -left_bytes;
    h_right.free_bytes = PAGE_SIZE - sizeof(Header) - right_bytes;
    

    h_left.save_header(&left_pg);
    h_right.save_header(&right_pg);

    dm.write_page(h_left.page_id,&left_pg);
    dm.write_page(right_pageId,&right_pg);

    //update the parentId of childs
    for (int i = 0; i <= h_right.keys_count; i++) {
        int child_id;
        memcpy(&child_id, dest_right + i * row_size , sizeof(int));
        Page child_pg;
        dm.read_page(child_id, &child_pg);
        Header h_child;
        h_child.load_header(&child_pg);
        h_child.parent_id = right_pageId;
        h_child.save_header(&child_pg);
        dm.write_page(child_id, &child_pg);
    }

    SplitInfo res ;
    res.right_page_id = right_pageId;
 
    char* key_ptr = (char*)full + rows_unshifted*row_size + sizeof(int);
    memcpy(res.separator_key, key_ptr, key_size);

    return res;
}

SplitInfo BplusTree::leaf_split(Disk_Manager &dm,Table_Metadata &tmd,Page &left_pg,const int &row_size,
    const char* full,const int &key_size,const int &key_off){

    Header h_left;
    Header h_right;

    h_left.load_header(&left_pg);
    
    Page right_pg;
    int right_pageId = dm.allocate_page(tmd);
    
    if(h_left.nextleaf!=0){
        Page next_pg;
        dm.read_page(h_left.nextleaf,&next_pg);
        Header h_next;
        h_next.load_header(&next_pg);
        h_next.prevleaf = right_pageId;
        h_next.save_header(&next_pg);
        dm.write_page(h_left.nextleaf ,&next_pg);
    }

    dm.init_leaf_page(&right_pg,right_pageId,h_left.parent_id,h_left.nextleaf,h_left.page_id);
    h_left.nextleaf = right_pageId;
    h_right.load_header(&right_pg);

    int total_rows  = h_left.keys_count+1;
    int rows_to_shift = total_rows/2;
    int rows_unshifted = total_rows - rows_to_shift;

    //shift the rows
    char * dest_left = (char*)&left_pg+sizeof(Header);
    char * dest_right = (char*)&right_pg+sizeof(Header);
    char * src_left = (char*)full;
    char * src_right = (char*)full + rows_unshifted*row_size;

    memcpy(dest_left,src_left,rows_unshifted*row_size);
    
    memcpy(dest_right,src_right,rows_to_shift*row_size);
    
    //update key count
    h_left.keys_count = rows_unshifted;
    h_right.keys_count = rows_to_shift;
    
    
    //update the free bytes
    h_left.free_bytes = PAGE_SIZE - sizeof(Header) - h_left.keys_count * row_size;
    h_right.free_bytes = PAGE_SIZE - sizeof(Header) - h_right.keys_count * row_size;
    
    char* left_free = dest_left + rows_unshifted*row_size;
    memset(left_free,0,h_left.free_bytes);

    char* right_free = dest_right  + rows_to_shift*row_size;
    memset(right_free,0,h_right.free_bytes);


    h_left.save_header(&left_pg);
    h_right.save_header(&right_pg);

    dm.write_page(h_left.page_id,&left_pg);
    dm.write_page(right_pageId,&right_pg);


    SplitInfo res ;
    res.right_page_id = right_pageId;
 
    char* first_row_right = (char*)&right_pg + sizeof(Header);
    char* key_ptr = first_row_right + key_off;
    memcpy(res.separator_key, key_ptr, key_size);

    return res;
}

////debugging
// void BplusTree::check_integrity(Disk_Manager &dm, Table_Metadata &tmd, int key_size) {
//     if (tmd.root_page_id == 0) return;
//     std::vector<int> queue;
//     queue.push_back(tmd.root_page_id);
//     int head = 0;

//     while(head < queue.size()){
//         int curr_id = queue[head++];
//         Page pg; Header h;
//         dm.read_page(curr_id, &pg);
//         h.load_header(&pg);

//         if(h.page_type == INTERNALNODE) {
//             // Check P0
//             int p0; memcpy(&p0, (char*)&pg + sizeof(Header), 4);
//             validate_child(dm, curr_id, p0, queue);

//             // Check P1...Pn
//             for(int i=0; i < h.keys_count; i++) {
//                 int child_ptr_offset = sizeof(Header) + 4 + (i * (key_size + 4)) + key_size;
//                 int pi; memcpy(&pi, (char*)&pg + child_ptr_offset, 4);
//                 validate_child(dm, curr_id, pi, queue);
//             }
//         }
//     }
// }

// void BplusTree::validate_child(Disk_Manager &dm, int parent_id, int child_id, std::vector<int> &queue) {
//     if (child_id <= 0) return;
//     Page pg; Header h;
//     dm.read_page(child_id, &pg);
//     h.load_header(&pg);
    
//     if(h.parent_id != parent_id) {
//         cout << "\n!!! INTEGRITY FAILURE !!!" << endl;
//         cout << "Page " << child_id << " (Type: " << h.page_type 
//              << ") claims Parent is " << h.parent_id 
//              << " but Parent Node " << parent_id << " points to it." << endl;
//         // Don't exit, just print so you can see the whole mess
//     }
    
//     if(h.page_type == INTERNALNODE) queue.push_back(child_id);
// }

// void BplusTree::print_tree_structure(Disk_Manager &dm, Table_Metadata &tmd, int key_size) {
//     if (tmd.root_page_id == 0) {
//         cout << "Tree is empty." << endl;
//         return;
//     }

//     std::vector<int> current_level;
//     current_level.push_back(tmd.root_page_id);
//     int level = 0;

//     cout << "\n--- B+ Tree Visualization ---" << endl;

//     while (!current_level.empty()) {
//         std::vector<int> next_level;
//         cout << "Level " << level++ << ": ";

//         for (int pid : current_level) {
//             Page pg;
//             Header h;
//             dm.read_page(pid, &pg);
//             h.load_header(&pg);

//             cout << "[P" << pid << " (Parent:" << h.parent_id << ") ";
            
//             if (h.page_type == INTERNALNODE) {
//                 // Print Pointers and Keys: P0 | K1 | P1 | K2 | P2...
//                 int p0;
//                 memcpy(&p0, (char*)&pg + sizeof(Header), 4);
//                 cout << "Ptrs:(" << p0;
//                 next_level.push_back(p0);

//                 for (int i = 0; i < h.keys_count; i++) {
//                     int key;
//                     int pi;
//                     int key_off = sizeof(Header) + 4 + (i * (key_size + 4));
//                     int ptr_off = key_off + key_size;
                    
//                     memcpy(&key, (char*)&pg + key_off, 4); // Assuming int32
//                     memcpy(&pi, (char*)&pg + ptr_off, 4);
                    
//                     cout << " | K:" << key << " | P:" << pi;
//                     next_level.push_back(pi);
//                 }
//                 cout << ")] ";
//             } else {
//                 // Leaf Node
//                 cout << "Leaf Keys: ";
//                 for (int i = 0; i < h.keys_count; i++) {
//                     int key;
//                     // Note: Change 20 to your actual row_size
//                     memcpy(&key, (char*)&pg + sizeof(Header) + (i * 20), 4); 
//                     cout << key << " ";
//                 }
//                 cout << "| Next:" << h.nextleaf << "] ";
//             }
//         }
//         cout << endl;
//         current_level = next_level;
//     }
// }



// int main(){
//     struct TestRow {
//     int id;
//     char data[16]; // Fixed size for testing
//     };

//     Disk_Manager dm;
//     BplusTree tree;
//     string tableName = "test_table";
    
//     // 1. Setup
//     dm.create_file(tableName);
//     int row_size = sizeof(TestRow); 
//     int key_size = sizeof(int);
//     int key_off = 0; // 'id' is at the start of TestRow

//     std::cout << "--- Initializing Tree ---" << std::endl;
//     tree.create_tree(dm, row_size, key_size);

//     // 2. Insert sequential data to force right-side splits
//     // With 128 byte pages, 20 insertions should create several levels
//     std::cout << "--- Inserting 100 Rows (Sequential) ---" << std::endl;
//     for (int i = 1; i <= 100; ++i) {
//         TestRow row;
//         row.id = i;
//         snprintf(row.data, sizeof(row.data), "val_%dllllllllll", i);
        
//         std::cout << "Inserting ID: " << i << "... ";
//         tree.insert_leaf(dm, datatype::int32, &row, key_size, row_size, key_off);
//         std::cout << "Done." << std::endl;

//         // Refresh Metadata
//         Table_Metadata tmd;
//         Page meta; dm.read_page(0, &meta);
//         tmd.load_table_md(&meta);

//         // Debug every split
//         if (i % 5 == 0) { 
//             cout << "\nChecking after " << i << " inserts..." << endl;
//             tree.check_integrity(dm, tmd, key_size);
//     }
//     }

//     // 3. Verification: Traverse the Clustered Leaf Level (Linked List)
//     std::cout << "\n--- Verifying Clustered Leaf Chain ---" << std::endl;
//     Page meta_pg;
//     dm.read_page(0, &meta_pg);
//     Table_Metadata tmd;
//     tmd.load_table_md(&meta_pg);

//     int current_leaf_id = tmd.first_leaf_page_id;
//     while (current_leaf_id != 0) {
//         Page leaf_pg;
//         Header h;
//         dm.read_page(current_leaf_id, &leaf_pg);
//         h.load_header(&leaf_pg);

//         std::cout << "Page [" << current_leaf_id << "] Keys: " << h.keys_count << " | Data: ";
        
//         for (int i = 0; i < h.keys_count; i++) {
//             int key;
//             memcpy(&key, (char*)&leaf_pg + sizeof(Header) + (i * row_size), sizeof(int));
//             std::cout << key << " ";
//         }
//         std::cout << " | Next Leaf: " << h.nextleaf << std::endl;
        
//         current_leaf_id = h.nextleaf;
//     }

//     // 4. Point Lookup Test
//     std::cout << "\n--- Testing Point Lookup ---" << std::endl;
//     int search_key = 41;
//     int target_page = tree.search_leaf(dm, tmd, datatype::int32, &search_key, key_size);
//     std::cout << "Key 41 should be in Page: " << target_page << std::endl;

    
//     tree.print_tree_structure(dm, tmd, key_size);

//     return 0;
// }
