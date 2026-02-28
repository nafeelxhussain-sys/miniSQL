#include<iostream>
#include<cstring>
#include<bits/stdc++.h>
#include "DiskManager.h"
#include "utils.h"
#include "BPlusTree.h"
using namespace std;

BplusTree::BplusTree(Disk_Manager &dm_ref, Table_Metadata &tmd_ref, datatype type, int k_size, bool is_primary_)
        : dm(dm_ref), tmd(tmd_ref), dt(type), key_size(k_size) ,is_primary(is_primary_){}

void BplusTree::sync_metadata() {
    Page meta_pg;
    tmd.save_table_md(&meta_pg);
    dm.write_page(0, &meta_pg);
}

void BplusTree :: open_tree(){
    Page meta_pg;
    dm.read_page(0,&meta_pg);
    tmd.load_table_md(&meta_pg);
}

void BplusTree::create_tree(int &row_size){
    
    tmd.internalnode_order = (PAGE_SIZE-sizeof(Header)-sizeof(int))/(key_size+sizeof(int));
    tmd.leafnode_order = (PAGE_SIZE-sizeof(Header))/row_size;

    Page buffer;

    //create and write the meta page
    sync_metadata();

    //allocate pageid for root
    int pid = dm.allocate_page(tmd);

    //create root
    dm.init_leaf_page(&buffer,pid);
    dm.write_page(pid,&buffer);
}


int BplusTree::search_leaf(const void *key){
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
            int key_off = sizeof(Header) + sizeof(int) + i*(sizeof(int)+key_size);
            int child_off = sizeof(Header)+i*(sizeof(int)+key_size);

            int compared = 0;
            if(is_primary){
                compared = compare_keys(data+key_off ,dt,key,key_size);
            }
            else{
                compared = compare_composite(data+key_off ,dt,key,key_size);
            }


            if(compared<0){
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

int BplusTree::find_in_leaf(const int &pageid ,const void *key,const int &row_size, const int &key_off){
    // binary search on the rows to get the position to insert
    //- represents that the key actually already exists 
    //+ the place where if it exists it should be at 

    Page buffer;
    dm.read_page(pageid,&buffer);

    Header h;
    h.load_header(&buffer);

    int end = h.keys_count-1;
    int start = 0;
    int mid = (end+start)/2; 
    
    while(end >= start){
        mid =(end + start)/2; 

        char* index = (char*)&buffer + row_size * mid + sizeof(Header) + key_off;

        int compared = 0;
        if(is_primary)
        compared = compare_keys(index,dt,key,key_size);
        //returns 1 when key is smaller
        else
        compared = compare_composite(index,dt,key,key_size);

        
        if(compared == 0){return -(mid+1);}
        else if(compared==1){
            start = mid+1;
        }else{
            end = mid-1;
        }
    }
    return start;
}

void BplusTree::insert_row( const void* row,const int &row_size, const int &key_off){
    
    Header h;
    Page pg;

    // dm.read_page(0,&pg);
    // tmd.load_table_md(&pg);
    const char* search_key = (char*)row + key_off;


    //find the page in which insertion is going to happen
    int leaf_id = search_leaf(search_key);

    dm.read_page(leaf_id,&pg);
    h.load_header(&pg);
    int parentId=h.parent_id;

    //find the rowId to insert
    int rId = find_in_leaf(leaf_id,search_key,row_size,key_off);

    if(rId < 0){
        //the key already exists and we cannot create a copy of pk
        //code tbu
        return ;
    }

    //update the parent seperator key
    if(rId==0){
        const char* old_key = (char*)&pg + sizeof(Header) + key_off;
        const char* new_key = (char*)row  + key_off;
        update_parent(h.parent_id,old_key,new_key);
    }

    bool overflow = h.free_bytes < row_size;

    if(!overflow){

        //insert the row 
        char* base = (char*)&pg + sizeof(Header);
        char* src  = base + rId * row_size;
        char* dest = base + (rId + 1) * row_size;

        int bytes_to_move = h.keys_count*row_size - rId*row_size;

        memmove(dest , src , bytes_to_move);
        memcpy(src,row,row_size);


        //update the header
        h.keys_count++;
        h.free_bytes -= row_size;

        h.save_header(&pg);


        //write the page back
        dm.write_page(leaf_id,&pg);

        sync_metadata();
        return;
    }

    //overflow
    //insert the row in the buffer
    int total_bytes = row_size * (h.keys_count+1);
    char* full = new char[total_bytes];

    char *old_base = (char*)&pg + sizeof(Header);
    memcpy(full , old_base, h.keys_count*row_size);

    char* base = (char*)full;
    char* src  = base + rId * row_size;
    char* dest = base + (rId + 1) * row_size;

    int bytes_to_move = h.keys_count*row_size - rId*row_size;

    memmove(dest , src , bytes_to_move);
    memcpy(src,row,row_size);


    //leaf split case
    SplitInfo res = leaf_split(pg, row_size,full,key_off);
    delete[] full;


    if(tmd.root_page_id == leaf_id){
        //create a new root
        create_new_root(leaf_id,res.right_page_id,res.separator_key);
        sync_metadata(); 
        return ;
    }

    //update the parent node
    insert_parent(h.parent_id,res.separator_key,res.right_page_id);

    sync_metadata();
}

void BplusTree::create_new_root(const int &leaf_id,const int &right_leaf_id,const void *seperator_key){

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
    sync_metadata();

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
    
    sync_metadata();
}

int BplusTree::find_in_parent(const int &parentId ,const void *key){
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

        int compared=0;
        if(is_primary)
        compared = compare_keys(index,dt,key,key_size);
        //returns 1 when key is smaller
        else
        compared = compare_composite(index,dt,key,key_size);
        
        if(compared == 0){return -(mid+1);}
        else if(compared==1){
            start = mid+1;
        }else{
            end = mid-1;
        }
    }
    return start;
}

void BplusTree::insert_parent(const int &pageId ,const void *key,const int &child){

    Header h;
    Page pg;

    // dm.read_page(0,&pg);
    // tmd.load_table_md(&pg);
    dm.read_page(pageId,&pg);
   
    h.load_header(&pg);
    int parentId=h.parent_id;
    int row_size = key_size + sizeof(int);

    //find the rowId to insert
    int rowId = find_in_parent(pageId,key);

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
    SplitInfo res = parent_split(pg,key,child,full);
    delete[] full;
    

    if(tmd.root_page_id == pageId){
        //create a new root
        create_new_root(pageId,res.right_page_id,res.separator_key); 
        return ;
    }

    //update the parent node
    insert_parent(h.parent_id,res.separator_key,res.right_page_id);
}

SplitInfo BplusTree::parent_split(Page &left_pg,const void * key,const int &child,const char* full){

    Header h_left;
    Header h_right;

    h_left.load_header(&left_pg);
    int row_size = key_size + sizeof(int);
    
    Page right_pg;
    int right_pageId = dm.allocate_page(this->tmd);
    
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

SplitInfo BplusTree::leaf_split(Page &left_pg,const int &row_size,const char* full,const int &key_off){

    Header h_left;
    Header h_right;

    h_left.load_header(&left_pg);
    
    Page right_pg;
    int right_pageId = dm.allocate_page(this->tmd);
    
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

void BplusTree::delete_row(const int &row_size, const void* key_ptr, const int &key_off){

    //find the leaf in which the row exists
    // Page buffer;
    // dm.read_page(0,&buffer);
    // tmd.load_table_md(&buffer);

    // cout<<"before search"<<endl;
    int leafId = search_leaf(key_ptr);
    int rowId = find_in_leaf(leafId,key_ptr,row_size,key_off);
    // cout<<"rid " <<rowId <<"leafId "<<leafId<<endl;
    
    //calculate the rowId
    if(rowId>=0){
        //key not found
        //tbu
        return;
    }
    
    
    
    rowId = -rowId - 1;
    //delete the row
    Page pg;
    Header h;
    
    dm.read_page(leafId,&pg);
    h.load_header(&pg);
    
    char *src = (char*)&pg + sizeof(Header) + (rowId+1)*row_size;
    char *dest = (char*)&pg + sizeof(Header) + (rowId)*row_size;
    int rows_to_move = h.keys_count - (rowId + 1);
    
    memmove(dest,src,rows_to_move*row_size);
    
    //update the header
    h.keys_count--;
    h.free_bytes+=row_size;
    
    char* free = (char*)&pg + sizeof(Header) + h.keys_count*row_size;
    memset(free, 0 , h.free_bytes);
    
    //save file and header
    
    h.save_header(&pg);
    dm.write_page(leafId,&pg);
    
    //update the seperator key of parent if rowId==0
    if(rowId==0 && h.keys_count>0){
        
        char* new_key = (char*)&pg + sizeof(Header) + key_off;
        
        update_parent(h.parent_id,key_ptr,new_key);
        
    }
    
    //check for underflow conditions
    int minimum_keys = (tmd.leafnode_order+1)/2;
    int underflow = false;
    if(h.keys_count < minimum_keys){
        underflow = true;
    }
    
    sync_metadata();
    if(!underflow) return;
    // cout<<"after search"<<endl;

    if(tmd.root_page_id == h.page_id){
        return;
    }

    Page &curr=pg;
    Page left;
    Page right;
    Header &h_curr = h;
    Header h_left;
    Header h_right;

    bool left_exists = (h_curr.prevleaf != 0);
    bool left_shares_parent = false;
    bool left_can_lend = false;

    if(left_exists){
        dm.read_page(h_curr.prevleaf,&left);
        h_left.load_header(&left);

        left_shares_parent = h_left.parent_id == h_curr.parent_id;
        left_can_lend = h_left.keys_count > minimum_keys;
    }

    bool right_exists = (h_curr.nextleaf != 0);
    bool right_shares_parent = false;
    bool right_can_lend = false;

    if(right_exists){
        dm.read_page(h_curr.nextleaf,&right);
        h_right.load_header(&right);
        right_shares_parent = h_right.parent_id == h_curr.parent_id;
        right_can_lend = h_right.keys_count > minimum_keys;

    }

    //case left borrow
    bool borrow_left_possible = left_exists && left_can_lend && left_shares_parent;
    if(borrow_left_possible){
        borrow_left(h.page_id,row_size,key_off);
        sync_metadata();
        return;
    }

    //case right borrow
    bool borrow_right_possible = right_exists && right_can_lend && right_shares_parent;
    if(borrow_right_possible){
        borrow_right(h.page_id,row_size,key_off);
        sync_metadata();
        return;
    }

    //case merge left
    bool merge_left_possible = left_exists && !left_can_lend && left_shares_parent;
    if(merge_left_possible){
        merge_leaf(h_curr.page_id,row_size,key_off);
        sync_metadata();
        return;
    }

    //case merge right
    bool merge_right_possible = right_exists && !right_can_lend && right_shares_parent;
    if(merge_right_possible){
        merge_leaf(h_curr.nextleaf,row_size,key_off);
        sync_metadata();
        return;
    }

    sync_metadata();
    return;
}

void BplusTree::update_parent(const int &parentId, const void *old_key, const void *new_key){

    if(parentId==0) return;

    int rowId = find_in_parent(parentId,old_key);
    int row_size = key_size+sizeof(int);

    if(rowId<0){
        //key found so update it
        rowId = -rowId -1;
        
        Page pg;
        dm.read_page(parentId,&pg);

        char* key_ptr = (char*) &pg + sizeof(Header) + sizeof(int) + rowId*row_size;

        memcpy(key_ptr , new_key , key_size);
        dm.write_page(parentId,&pg);
        return;
    }

    Page pg;
    dm.read_page(parentId,&pg);
    Header h;
    h.load_header(&pg);
    int grandParentId = h.parent_id;

    //recursive call
    update_parent(grandParentId, old_key,new_key);
}

void BplusTree::borrow_left(const int &pageId,const int &row_size, const int &key_off){
    Page left;
    Page curr;
    Header h_curr;
    Header h_left;

    dm.read_page(pageId,&curr);
    h_curr.load_header(&curr);

    dm.read_page(h_curr.prevleaf,&left);
    h_left.load_header(&left);

    char* src = (char*)&curr +sizeof(Header);
    char* dest = (char*)&curr +sizeof(Header)+row_size;
    int bytes_to_move = row_size*h_curr.keys_count;

    char old_key[key_size]; 
    memcpy(old_key, src + key_off, key_size);

    memmove(dest,src,bytes_to_move);

    dest = (char*)&curr +sizeof(Header);
    src = (char*)&left +sizeof(Header) + row_size*(h_left.keys_count-1);

    memcpy(dest,src,row_size);


    //update the header
    h_left.keys_count--;
    h_left.free_bytes+=row_size;
    h_curr.keys_count++;
    h_curr.free_bytes-=row_size;

    h_left.save_header(&left);
    h_curr.save_header(&curr);

    char* left_free = (char*)&left +sizeof(Header) + row_size*(h_left.keys_count);
    char* curr_free = (char*)&curr +sizeof(Header) + row_size*(h_curr.keys_count);
    memset(left_free,0,h_left.free_bytes);
    memset(curr_free,0,h_curr.free_bytes);

    dm.write_page(h_curr.page_id,&curr);
    dm.write_page(h_left.page_id,&left);


    //update parent seperator key
    const char* new_key = (char*)&curr + sizeof(Header) + key_off;  
    update_parent(h_curr.parent_id,old_key,new_key);
    
}

void BplusTree::borrow_right(const int &pageId,const int &row_size, const int &key_off){

    Page curr;
    Page right;
    Header h_right;
    Header h_curr;

    dm.read_page(pageId,&curr);
    h_curr.load_header(&curr);

    dm.read_page(h_curr.nextleaf,&right);
    h_right.load_header(&right);

    char* src = (char*)&right +sizeof(Header);
    char* dest = (char*)&curr +sizeof(Header)+h_curr.keys_count*row_size;
        
    char old_key[key_size];
    memcpy(&old_key, src+key_off,key_size);

    memcpy(dest,src,row_size);

    dest = (char*)&right +sizeof(Header);
    src = (char*)&right+sizeof(Header)+row_size;

    memmove(dest,src,row_size*(h_right.keys_count-1));


    //update the header
    h_right.keys_count--;
    h_right.free_bytes+=row_size;
    h_curr.keys_count++;
    h_curr.free_bytes-=row_size;

    char* right_free = (char*)&right +sizeof(Header) + row_size*(h_right.keys_count);
    char* curr_free = (char*)&curr +sizeof(Header) + row_size*(h_curr.keys_count);
    memset(right_free,0,h_right.free_bytes);
    memset(curr_free,0,h_curr.free_bytes);

    h_right.save_header(&right);
    h_curr.save_header(&curr);

    dm.write_page(h_curr.page_id,&curr);
    dm.write_page(h_right.page_id,&right);


    //update parent seperator key
    const char* new_key = (char*)&right + sizeof(Header) + key_off;   
    update_parent(h_curr.parent_id,old_key,new_key);

}

void BplusTree::merge_leaf(const int &pageId,const int &row_size, const int &key_off){
    
    Page curr;
    Page left;
    Header h_left;
    Header h_curr;

    dm.read_page(pageId,&curr);
    h_curr.load_header(&curr);


    dm.read_page(h_curr.prevleaf,&left);
    h_left.load_header(&left);

    //move all rows from curr to left and delete curr

    //setup the src to the start of rows in curr page
    //setup the dest to the end of rows in left page 
    char* src = (char*)&curr + sizeof(Header);
    char* dest = (char*)&left + sizeof(Header) + row_size*h_left.keys_count;
    int bytes_to_move = h_curr.keys_count*row_size;

    //store the original key to be deleted
    char deletion_key[key_size];
    memcpy(deletion_key,src+key_off,key_size);

    memcpy(dest,src,bytes_to_move);

    //update keycount and links
    h_left.keys_count += h_curr.keys_count;
    h_left.nextleaf = h_curr.nextleaf;
    h_left.free_bytes -= h_curr.keys_count*row_size;
    if(h_curr.nextleaf!=0){
        //update the prev link as well
        Page pg;
        Header h;
        dm.read_page(h_curr.nextleaf, &pg);
        h.load_header(&pg);
        h.prevleaf = h_left.page_id;
        h.save_header(&pg);
        dm.write_page(h.page_id,&pg);
    }


    h_left.save_header(&left);
    dm.write_page(h_left.page_id, &left);

    
    dm.free_page(tmd,h_curr.page_id);

    //delete the seperator key in parent
    delete_parent(h_curr.parent_id,deletion_key);
}

void BplusTree::delete_parent(const int &pageId,const char* seperator_key){

    Page buffer;
    // Page tmd_data;
    Header h;

    dm.read_page(pageId,&buffer);
    h.load_header(&buffer);
    // dm.read_page(0,&tmd_data);
    // tmd.load_table_md(&tmd_data);
    
    //find the parent key 
    int rowId = find_in_parent(pageId,seperator_key);

    if(rowId>=0){
        //key not found
        return;
    }


    rowId = -rowId -1;

    int row_size = key_size+sizeof(int);

    char* dest = (char*)&buffer + sizeof(Header) + sizeof(int) + rowId*row_size;
    char* src = (char*)&buffer + sizeof(Header) + sizeof(int) + (rowId+1)*row_size;
    int bytes_to_move = (h.keys_count-rowId-1)*row_size;

    memmove(dest,src,bytes_to_move);

    h.keys_count--;
    h.free_bytes+=row_size;

    char* free = (char*)&buffer + sizeof(Header) + sizeof(int) + (h.keys_count)*row_size;
    memset(free,0,h.free_bytes);

    h.save_header(&buffer);
    dm.write_page(pageId,&buffer);


    int minimum_keys = (tmd.internalnode_order +1)/2;
    bool underflow = false;
    if(h.keys_count < minimum_keys){
        underflow = true;
    }

    if(!underflow) return;

    //check root
    if(tmd.root_page_id == h.page_id){
        if(h.keys_count == 0&& h.page_type==INTERNALNODE) 
        delete_root();
        return;
    }


    char* search_key =  (char*)&buffer + sizeof(Header) + sizeof(int); 
    int parent_sep_key_rowId = find_in_parent(h.parent_id,search_key);

    Page parent_buffer;
    Header h_parent;
    dm.read_page(h.parent_id,&parent_buffer);
    h_parent.load_header(&parent_buffer);

    //extract right, left sibling pageId
    int right_sibling_pageId=0 ;
    int left_sibling_pageId =0;

    if(parent_sep_key_rowId < h_parent.keys_count){
        char* right_sibling_pointer = (char*)&parent_buffer +sizeof(Header) + parent_sep_key_rowId*row_size + row_size;
        memcpy(&right_sibling_pageId,right_sibling_pointer,sizeof(int));
    }

    if(parent_sep_key_rowId>0){
        char* left_sibling_pointer = (char*)&parent_buffer +sizeof(Header) + (parent_sep_key_rowId)*row_size -row_size;
        memcpy(&left_sibling_pageId,left_sibling_pointer,sizeof(int));
    }


    Page left,right;
    Header h_left,h_right;

    bool left_exists = false;
    bool left_can_lend = false;
    if(left_sibling_pageId!=0){
        dm.read_page(left_sibling_pageId,&left);
        h_left.load_header(&left);

        left_exists=true;
        left_can_lend = h_left.keys_count > minimum_keys;
    }

    bool right_exists = false;
    bool right_can_lend = false;
    if(right_sibling_pageId!=0){
        dm.read_page(right_sibling_pageId,&right);
        h_right.load_header(&right);

        right_exists=true;
        right_can_lend = h_right.keys_count > minimum_keys;
    }

    //borrow left
    bool borrow_left_possible=left_exists && left_can_lend;
    if(borrow_left_possible){
        borrow_left_parent(h.page_id,h_left.page_id,parent_sep_key_rowId-1);
        return ;
    }

    //borrow righ
    bool borrow_right_possible = right_exists && right_can_lend;
    if( borrow_right_possible){
        borrow_right_parent(h.page_id,h_right.page_id,parent_sep_key_rowId);
        return ;
    }

    //merge left
    bool merge_left_possible = left_exists && !left_can_lend;
    if(merge_left_possible){
        merge_parent(h_left.page_id,h.page_id);
        return;
    }

    //merge right
    bool merge_right_possible = right_exists && !right_can_lend;
    if(merge_right_possible){
        merge_parent(h.page_id,h_right.page_id);
        return;
    }

}

void BplusTree::borrow_right_parent(const int &pageId,const int &rightId,const int&rowId){

    Header h_right,h_curr,h_parent;
    Page right,curr,parent;

    dm.read_page(pageId,&curr);
    h_curr.load_header(&curr);

    dm.read_page(h_curr.parent_id,&parent);
    h_parent.load_header(&parent);

    int row_size = key_size+sizeof(int);
    dm.read_page(rightId,&right);
    h_right.load_header(&right);

    // 1. KEY ROTATION: Pull from parent to current
    char* parent_sep_key = (char*)&parent + sizeof(Header) + sizeof(int) + rowId * row_size;
    char* curr_dest_key = (char*)&curr + sizeof(Header) + sizeof(int) + h_curr.keys_count * row_size;
    memcpy(curr_dest_key, parent_sep_key, key_size);

    // 2. POINTER ROTATION: Right Sibling's P0 moves to Current's new rightmost slot
    char* right_p0_ptr = (char*)&right + sizeof(Header); 
    char* curr_dest_ptr = curr_dest_key + key_size;
    memcpy(curr_dest_ptr, right_p0_ptr, sizeof(int));

    //Moved child
    int moved_child_id;
    memcpy(&moved_child_id, right_p0_ptr, sizeof(int));

    // 3. SEPARATOR UPDATE: Right Sibling's K0 moves to Parent
    char* right_k0_ptr = (char*)&right + sizeof(Header) + sizeof(int); 
    memcpy(parent_sep_key, right_k0_ptr, key_size);

    // 4. SIBLING REORGANIZE: Right Sibling's P1 becomes its new P0
    char* right_p1_ptr = right_k0_ptr + key_size;
    memcpy(right_p0_ptr, right_p1_ptr, sizeof(int));

    // 5. SHIFT SIBLING: Move remaining (K1, P2...) to (K0, P1...)
    char* sib_dest = (char*)&right + sizeof(Header) + sizeof(int);
    char* sib_src = sib_dest + row_size;
    int bytes_to_move = (h_right.keys_count - 1) * row_size;
    memmove(sib_dest, sib_src, bytes_to_move);

    //update headers
    h_curr.keys_count++;
    h_curr.free_bytes-=row_size;
    h_right.keys_count--;
    h_right.free_bytes+=row_size;

    h_right.save_header(&right);
    h_curr.save_header(&curr);
    h_parent.save_header(&parent);

    //update parent of child
    Page child;
    Header h_child;
    dm.read_page(moved_child_id,&child);
    h_child.load_header(&child);
    h_child.parent_id = h_curr.page_id;
    h_child.save_header(&child);


    dm.write_page(h_curr.page_id,&curr);
    dm.write_page(h_right.page_id,&right);
    dm.write_page(h_parent.page_id,&parent);
    dm.write_page(h_child.page_id,&child);
}

void BplusTree::borrow_left_parent(const int &pageId,const int &leftId,const int&rowId){

    Header h_left,h_curr,h_parent;
    Page left,curr,parent;

    dm.read_page(pageId,&curr);
    h_curr.load_header(&curr);

    dm.read_page(h_curr.parent_id,&parent);
    h_parent.load_header(&parent);

    int row_size = key_size+sizeof(int);
    dm.read_page(leftId,&left);
    h_left.load_header(&left);

    //  CURRENT SHIFTING: Move remaining (P0, K1, P1, ...) to (P1, K2, ...)
    char* curr_p0_start = (char*)&curr + sizeof(Header);
    char* curr_dest = curr_p0_start + row_size;
    int bytes_to_move = (h_curr.keys_count ) * row_size + sizeof(int);
    memmove(curr_dest, curr_p0_start, bytes_to_move);

    //  KEY ROTATION: Pull from parent to current
    char* parent_sep_key = (char*)&parent + sizeof(Header) + sizeof(int) + rowId * row_size;
    char* curr_new_k0_ptr = curr_p0_start + sizeof(int) ;
    memcpy(curr_new_k0_ptr, parent_sep_key, key_size);

    //  POINTER ROTATION: Left Sibling's P_last moves to Current P0 slot
    char* left_p_last_ptr = (char*)&left + sizeof(Header) + h_left.keys_count*row_size; 
    memcpy(curr_p0_start, left_p_last_ptr, sizeof(int));

    //Moved child
    int moved_child_id;
    memcpy(&moved_child_id, left_p_last_ptr, sizeof(int));

    //  SEPARATOR UPDATE: LEFT Sibling's K_last moves to Parent
    char* left_k_last_ptr = (char*)&left + sizeof(Header) + sizeof(int) + (h_left.keys_count-1)*row_size; 
    memcpy(parent_sep_key, left_k_last_ptr, key_size);

    //  LEFT REORGANIZE: Left Sibling's last Key and pointer removed
    memset(left_k_last_ptr, 0, row_size);

    

    //update headers
    h_curr.keys_count++;
    h_curr.free_bytes-=row_size;
    h_left.keys_count--;
    h_left.free_bytes+=row_size;

    h_left.save_header(&left);
    h_curr.save_header(&curr);
    h_parent.save_header(&parent);

    //update parent of child
    Page child;
    Header h_child;
    dm.read_page(moved_child_id,&child);
    h_child.load_header(&child);
    h_child.parent_id = h_curr.page_id;
    h_child.save_header(&child);


    dm.write_page(h_curr.page_id,&curr);
    dm.write_page(h_left.page_id,&left);
    dm.write_page(h_parent.page_id,&parent);
    dm.write_page(h_child.page_id,&child);
}

void BplusTree::merge_parent(const int &leftId,const int &rightId){
    
    //put all data from right into left
    Page left,right,parent;
    Header h_left, h_right,h_parent;

    dm.read_page(leftId,&left);
    dm.read_page(rightId,&right);
    h_left.load_header(&left);
    h_right.load_header(&right);

    dm.read_page(h_left.parent_id,&parent);
    h_parent.load_header(&parent);

    char* search_key = (char*)&left + sizeof(Header) + sizeof(int);
    int row_idx = find_in_parent(h_parent.page_id,search_key);
    int row_size = key_size+sizeof(int);

    //pulldown seperation key
    char* parent_sep_key_ptr = (char*)&parent + sizeof(Header) + sizeof(int) + row_idx*row_size;
    char* dest = (char*)&left + sizeof(Header) + sizeof(int) + h_left.keys_count*row_size;
    memcpy(dest,parent_sep_key_ptr, key_size);
 
    char deletion_key[key_size];
    memcpy(deletion_key,parent_sep_key_ptr,key_size);

    //move all data from right to left
    char* right_start = (char*)&right + sizeof(Header);
    dest+=key_size;
    int bytes_to_move = h_right.keys_count*row_size+sizeof(int);
    memcpy(dest,right_start,bytes_to_move);

    //update header
    h_left.keys_count+= 1 + h_right.keys_count;
    h_left.free_bytes-= (h_right.keys_count+1)*row_size;

    h_left.save_header(&left);
    dm.write_page(h_left.page_id,&left);

    //update children of right to correct parent
    char* right_child_ptr = right_start;
    for(int i = 0 ; i <= h_right.keys_count ;i++){
        int right_childs_pageId = 0;
        memcpy(&right_childs_pageId, right_child_ptr,sizeof(int));
        Page child;
        Header c;
        dm.read_page(right_childs_pageId,&child);
        c.load_header(&child);
        c.parent_id=h_left.page_id;
        c.save_header(&child);
        dm.write_page(c.page_id,&child);
        right_child_ptr+=row_size;
    }

    //free right
    dm.free_page(this->tmd,h_right.page_id);


    //delete the sep key in parent 
    delete_parent(h_parent.page_id,deletion_key);
}

void BplusTree::delete_root(){
    Page root;
    int new_root_id;

    dm.read_page(tmd.root_page_id,&root);
    char* new_root_ptr = (char*)&root + sizeof(Header);
    memcpy(&new_root_id , new_root_ptr,sizeof(int));

    Page new_root;
    dm.read_page(new_root_id,&new_root);
    Header new_root_header;
    new_root_header.load_header(&new_root);
    new_root_header.parent_id=0;
    new_root_header.save_header(&new_root);
    dm.write_page(new_root_header.page_id,&new_root);

    //free old root
    dm.free_page(this->tmd,tmd.root_page_id);

    //update tmd
    tmd.root_page_id=new_root_id;

    sync_metadata();
}

void BplusTree::scan_all(const int &row_size, function<void(const void*)> callback){
    int curr_page = tmd.first_leaf_page_id;

    while(curr_page!=0){
        // load header and page
        Header h;
        Page pg;

        dm.read_page(curr_page, &pg);
        h.load_header(&pg);

        char* row_ptr = (char*)&pg  +  sizeof(Header);

        // read whole page
        for(int i = 0 ; i<h.keys_count ; i++){
            callback(row_ptr);
            row_ptr += row_size;
        }


        //move to next page
        curr_page = h.nextleaf;
    }
}

void BplusTree::scan_forward(const int &row_size, const char* key,const int &key_off, function<void(const void*)> callback){
    int curr_page = search_leaf(key);
    bool first_page = true;

    while(curr_page!=0){
        // load header and page
        Header h;
        Page pg;

        dm.read_page(curr_page, &pg);
        h.load_header(&pg);

        char* row_ptr = (char*)&pg  +  sizeof(Header);

        // read pages
        int i = 0;
        if(first_page){
            i = find_in_leaf(curr_page,key,row_size,key_off);

            if(i<0)
            i = -i-1;
            
            row_ptr += i*row_size;
            first_page = false;
        }

        while(i<h.keys_count){
            callback(row_ptr);
            row_ptr += row_size;
            i++;
        }


        //move to next page
        curr_page = h.nextleaf;
    }
}

void BplusTree::scan_backward(const int &row_size, const char* key,const int &key_off, function<void(const void*)> callback){
    int curr_page = search_leaf(key);
    bool first_page = true;

    while(curr_page!=0){
        // load header and page
        Header h;
        Page pg;

        dm.read_page(curr_page, &pg);
        h.load_header(&pg);

        
        // read pages
        int row_idx = h.keys_count-1;
        if(first_page){
            int found_pos = find_in_leaf(curr_page, key, row_size, key_off);
            
            if (found_pos < 0) 
            row_idx = (-found_pos) - 1; 
            else 
            row_idx = found_pos ; 
            
            first_page = false;
        }
        
        char* row_ptr = (char*)&pg  +  sizeof(Header) + row_idx*row_size;

        while(row_idx>=0){
            callback(row_ptr);
            row_ptr -= row_size;
            row_idx--;
        }


        //move to prev page
        curr_page = h.prevleaf;
    }
}

void BplusTree::scan_point(const int &row_size, const char* key,const int &key_off, function<void(const void*)> callback){
    int curr_page = search_leaf(key);
    int curr_row = find_in_leaf(curr_page,key,row_size,key_off);

    if(curr_row >= 0){
        //error key not found
    }

    curr_row = -curr_row - 1;

    // load header and page
    Header h;
    Page pg;

    dm.read_page(curr_page, &pg);
    h.load_header(&pg);

    if (curr_row >= 0 && curr_row < h.keys_count) {
        char* row_ptr = (char*)&pg + sizeof(Header) + (curr_row  * row_size);
        callback(row_ptr);
    }
}


void BplusTree::update_row(const void *key, const int &row_size,const int &key_off, const int &col_off, const int &col_size, const char* updated_value){
    //find the page and row
    int target_page = search_leaf(key);
    int target_row = find_in_leaf(target_page,key,row_size,key_off);

    if(target_row >= 0 ){
        // row not fouond
    }

    target_row = -target_row - 1;

    // load header and page
    Header h;
    Page pg;

    dm.read_page(target_page, &pg);
    h.load_header(&pg);

    char* row_ptr = (char*)&pg  +  sizeof(Header) + row_size * target_row;
    char* col_ptr = row_ptr + col_off;

    memcpy(col_ptr, updated_value , col_size);
    dm.write_page(target_page, &pg);
}


// // Helper to visualize the raw bytes as readable data
// void debug_print_row(const void* data) {
//     int id;
//     char name[21]; // +1 for null terminator
//     memset(name, 0, 21);

//     // Extract ID from the first 4 bytes
//     memcpy(&id, data, sizeof(int));
//     // Extract Name from the next 20 bytes
//     memcpy(name, (char*)data + sizeof(int), 20);

//     cout << "  Row Data -> [ID: " << id << " | Name: " << name << "]" << endl;
// }

// int main() {
//     // 1. Initializing System Components
//     Disk_Manager dm;
//     Table_Metadata tmd;
    
//     string table_name = "production_test";
//     dm.create_file(table_name); // Your fixed function
    
//     // Config: ID (int, 4 bytes) + Name (char[20])
//     int row_size = 24; 
//     int key_size = 4;
//     int key_off = 0;
//     int name_off = 4;
//     int name_size = 20;

//     // Use the enum you've defined for int32
//     BplusTree tree(dm, tmd, datatype::int32, key_size, true);
//     tree.create_tree(row_size);

//     cout << "--- Phase 1: Heavy Insertion (Triggering Splits) ---" << endl;
//     for (int i = 1; i <= 50; i++) {
//         char buffer[24];
//         memset(buffer, 0, 24);

//         // Pack the integer ID into the first 4 bytes
//         memcpy(buffer, &i, sizeof(int));

//         // Pack the string into the next 20 bytes
//         string name_str = "Employee_" + to_string(i);
//         strncpy(buffer + name_off, name_str.c_str(), name_size);

//         tree.insert_row(buffer, row_size, key_off);
//     }
//     cout << "Inserted 50 records." << endl;

//     cout << "\n--- Phase 2: Sequential Scan (Scan All) ---" << endl;
//     tree.scan_all(row_size, debug_print_row);

//     cout << "\n--- Phase 3: Point Lookup (ID 25) ---" << endl;
//     int search_id = 25;
//     // Cast the address of the int to char* so the tree can compare bytes
//     tree.scan_point(row_size, (char*)&search_id, key_off, debug_print_row);

//     cout << "\n--- Phase 4: In-Place Update (Updating Name of ID 25) ---" << endl;
//     // Note: PK (ID) is immutable as per your design
//     const char* updated_name = "Manager_Senior_25"; 
//     tree.update_row((char*)&search_id, row_size, key_off, name_off, name_size, updated_name);
    
//     cout << "Verifying Update:" << endl;
//     tree.scan_point(row_size, (char*)&search_id, key_off, debug_print_row);

//     cout << "\n--- Phase 5: Range Scan Forward (ID >= 45) ---" << endl;
//     int start_id = 45;
//     tree.scan_forward(row_size, (char*)&start_id, key_off, debug_print_row);

//     cout << "\n--- Phase 6: Bounded Scan (ID < 10) ---" << endl;
//     int end_id = 10;
//     tree.scan_backward(row_size, (char*)&end_id, key_off, debug_print_row);

//     cout << "\n--- SUCCESS: Engine Verified ---" << endl;
//     return 0;
// }