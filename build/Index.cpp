#include<iostream>
#include <functional>
#include<cstring>
#include"DiskManager.h"
#include"BPlusTree.h"
#include "Index.h"
#include "utils.h"
using namespace std;

Index ::Index(Disk_Manager &dm, Table_Metadata &tmd, datatype dt, int pk_size, int index_size):
    tree(dm,tmd,dt,pk_size + index_size,false){// +pksize
        this->index_size = index_size;
        this->pk_size = pk_size;
}

void Index::open_index(){
    tree.open_tree();
}

void Index :: create_index(){
    int size = index_size+pk_size;
    tree.create_tree(size);
}

void Index::insert_index(const void* row){
    int key_off = 0;
    const int row_size = index_size + pk_size;
    tree.insert_row(row,row_size,key_off);
}

void Index::delete_index(const void* row){
    int key_off = 0;
    int row_size = index_size + pk_size;
    tree.delete_row(row_size,row,key_off);
}

void Index::update_index(const void* old_, const void* new_){
    delete_index(old_);
    insert_index(new_);
}

int Index::search_lower_bound(const void* target_value, datatype dt){
    Page buffer;
    Header h;

    int current_pid = tree.tmd.root_page_id;
    pagetype current_pagetype;

    char* data = (char*)&buffer;
    int depth = 0;
    int row_size = index_size + pk_size;

    while(true){
        if(++depth > 32) return 0;

        tree.dm.read_page(current_pid,&buffer);
        h.load_header(&buffer);

        current_pagetype = h.page_type;
        if(current_pagetype == LEAFNODE) break;

        int next_pid = 0;
        for(int i = 0 ; i < h.keys_count ; i++){
            int key_off = sizeof(Header) + sizeof(int) + i*(sizeof(int)+row_size);
            int child_off = sizeof(Header)+i*(sizeof(int)+row_size);

            int compared = compare_keys(data+key_off, dt, target_value, index_size);
            
            if(compared<=0){
                //returns -1 if search_k<key
                //returns 0 if search_k==key
                memcpy(&next_pid, data + child_off,sizeof(int));
                break;
            }
        }

        if(next_pid==0){
            int child_off = (sizeof(Header)+h.keys_count*(4+row_size));
            memcpy(&next_pid, data + child_off,4);
        }

        if(next_pid==0) return 0;

        current_pid = next_pid;
    }

    return current_pid;
}


int Index::search_upper_bound(const void* target_value, datatype dt){
    Page buffer;
    Header h;

    int current_pid = tree.tmd.root_page_id;
    pagetype current_pagetype;

    char* data = (char*)&buffer;
    int depth = 0;
    int row_size = index_size + pk_size;

    while(true){
        if(++depth > 32) return 0;

        tree.dm.read_page(current_pid,&buffer);
        h.load_header(&buffer);

        current_pagetype = h.page_type;
        if(current_pagetype == LEAFNODE) break;

        int next_pid = 0;
        for(int i = 0 ; i < h.keys_count ; i++){
            int key_off = sizeof(Header) + sizeof(int) + i*(sizeof(int)+row_size);
            int child_off = sizeof(Header)+i*(sizeof(int)+row_size);

            int compared = compare_keys(data+key_off, dt, target_value, index_size);
            
            if(compared<0){
                //returns -1 if search_k<key
                //returns 0 if search_k==key
                //returns 1 if search_k>key
                memcpy(&next_pid, data + child_off,sizeof(int));
                break;
            }
        }

        if(next_pid==0){
            int child_off = (sizeof(Header)+h.keys_count*(4+row_size));
            memcpy(&next_pid, data + child_off,4);
        }

        if(next_pid==0) return 0;

        current_pid = next_pid;
    }

    return current_pid;
}

void Index::find_all_pks(const void* target_value, datatype dt,function<void(const char*)> callback){
    int PageId = search_lower_bound(target_value,dt);
    if(PageId==0) return ;

    Page pg;
    Header h;
    
    int current_pid = PageId;
    int row_size = index_size + pk_size;
    bool found_at_least_one = false;

    while (current_pid != 0) {
        tree.dm.read_page(current_pid, &pg);
        h.load_header(&pg);

        // For the first page we need to find where the duplicates actually start
        char* row_ptr = (char*)&pg + sizeof(Header);

        for (int rowId = 0; rowId < h.keys_count; rowId++) {
            // Check if the value still matches
            int compared = compare_keys(row_ptr, dt, target_value, index_size);

            if (compared == 0) {
                found_at_least_one = true;

                // Extract PK pointer 
                char* pk_ptr = row_ptr + index_size;

                // callback
                callback(pk_ptr); 
            } 
            else if (found_at_least_one || compared < 0) {
                // Value changed after a match, range is over
                return;
            }
            
            row_ptr += row_size;
        }

        // Move to the next sibling leaf
        current_pid = h.nextleaf; 
    }
    return ;
}

void Index::find_all_pks_forward(const void* target_value, datatype dt,function<void(const char*)> callback){
    int PageId = search_lower_bound(target_value,dt);
    if(PageId==0) return ;

    Page pg;
    Header h;
    
    int current_pid = PageId;
    int row_size = index_size + pk_size;
    
    
    while (current_pid != 0) {
        tree.dm.read_page(current_pid, &pg);
        h.load_header(&pg);

        // For the first page we need to find where the duplicates actually start
        char* row_ptr = (char*)&pg + sizeof(Header);

        for (int rowId = 0; rowId < h.keys_count; rowId++) {
            // Check if the value still matches
            int compared = compare_keys(row_ptr, dt, target_value, index_size);
            if (compared <= 0) {
                // Extract PK pointer 
                char* pk_ptr = row_ptr + index_size;

                // callback
                callback(pk_ptr); 
            } 
            
            row_ptr += row_size;
        }

        // Move to the next sibling leaf
        current_pid = h.nextleaf; 
    }
    return ;
}

void Index::find_all_pks_backward(const void* target_value, datatype dt,function<void(const char*)> callback){
    int PageId = search_upper_bound(target_value,dt);
    if(PageId==0) return ;

    Page pg;
    Header h;
    
    int current_pid = PageId;
    int row_size = index_size + pk_size;
    
    
    while (current_pid != 0) {
        tree.dm.read_page(current_pid, &pg);
        h.load_header(&pg);

        // For the first page we need to find where the duplicates actually start
        char* row_ptr = (char*)&pg + sizeof(Header) + (h.keys_count-1) * row_size;

        for (int rowId = h.keys_count-1; rowId >= 0; rowId--) {
            // Check if the value still matches
            int compared = compare_keys(row_ptr, dt, target_value, index_size);
            if (compared >= 0) {
                // Extract PK pointer 
                char* pk_ptr = row_ptr + index_size;

                // callback
                callback(pk_ptr); 
            } 
            
            row_ptr -= row_size;
        }

        // Move to the prefv sibling leaf
        current_pid = h.prevleaf; 
    }
    return ;
}





// // --- Helper: Extract and Print from Raw Bytes ---
// void print_main_row(const void* data) {
//     int id, age;
//     char name[21]; 
//     memset(name, 0, 21);

//     memcpy(&id, data, 4);               // Offset 0: ID (PK)
//     memcpy(&age, (char*)data + 4, 4);   // Offset 4: Age
//     memcpy(name, (char*)data + 8, 20);  // Offset 8: Name
    
//     cout << "  [Fetched] ID: " << id << " | Age: " << age << " | Name: " << name << endl;
// }

// #include <queue>

// void Index::print_level_order() {
//     if (tree.tmd.root_page_id == 0) {
//         cout << "Tree is empty." << endl;
//         return;
//     }

//     // Queue stores {page_id, depth}
//     queue<pair<int, int>> q;
//     q.push({tree.tmd.root_page_id, 0});

//     int current_level = -1;

//     cout << "\n========== LEVEL ORDER DUMP ==========" << endl;

//     while (!q.empty()) {
//         pair<int, int> front = q.front();
//         q.pop();

//         int pid = front.first;
//         int depth = front.second;

//         // Print level separator
//         if (depth > current_level) {
//             current_level = depth;
//             cout << "\n--- LEVEL " << current_level << " ---" << endl;
//         }

//         Page pg;
//         Header h;
//         tree.dm.read_page(pid, &pg);
//         h.load_header(&pg);

//         cout << "[Page " << pid << " | " 
//              << (h.page_type == LEAFNODE ? "LEAF" : "INTERNAL") 
//              << " | Keys: " << h.keys_count << "] ";

//         if (h.page_type == INTERNALNODE) {
//             int internal_entry_size = 4 + index_size + pk_size; // 12 bytes
            
//             // Print all pivots in this internal node
//             cout << "Pivots: ";
//             for (int i = 0; i < h.keys_count; i++) {
//                 int pivot_age, pivot_pk;
//                 int key_off = sizeof(Header) + (i * internal_entry_size) + 4;
//                 memcpy(&pivot_age, (char*)&pg + key_off, 4);
//                 memcpy(&pivot_pk, (char*)&pg + key_off + 4, 4);
//                 cout << "(" << pivot_age << "," << pivot_pk << ") ";

//                 // Push children to queue for next level
//                 int child_pid;
//                 memcpy(&child_pid, (char*)&pg + (i * internal_entry_size) + sizeof(Header), 4);
//                 q.push({child_pid, depth + 1});
//             }
            
//             // Don't forget the rightmost child!
//             int last_child_pid;
//             int last_child_off = sizeof(Header) + (h.keys_count * internal_entry_size);
//             memcpy(&last_child_pid, (char*)&pg + last_child_off, 4);
//             q.push({last_child_pid, depth + 1});
            
//             cout << "| Last Child: " << last_child_pid << endl;

//         } else {
//             // It's a leaf, just show first and last keys to keep output clean
//             int entry_size = index_size + pk_size;
//             int first_age, last_age;
//             memcpy(&first_age, (char*)&pg + sizeof(Header), 4);
//             memcpy(&last_age, (char*)&pg + sizeof(Header) + (h.keys_count - 1) * entry_size, 4);
//             cout << "Range: [" << first_age << " to " << last_age << "]" << endl;
//         }
//     }
//     cout << "\n======================================" << endl;
// }

// void Index::print_tree_recursive(int pid, int level) {
//     Page pg;
//     Header h;
//     tree.dm.read_page(pid, &pg);
//     h.load_header(&pg);

//     // Indentation for tree levels
//     string indent = "";
//     for(int i = 0; i < level; i++) indent += "    ";

//     cout << indent << "[Page " << pid << "] type: " 
//          << (h.page_type == LEAFNODE ? "LEAF" : "INTERNAL") 
//          << " keys: " << h.keys_count << endl;

//     int entry_size = index_size + pk_size;

//     if (h.page_type == LEAFNODE) {
//         for (int i = 0; i < h.keys_count; i++) {
//             char* row_ptr = (char*)&pg + sizeof(Header) + (i * entry_size);
//             int age, id;
//             memcpy(&age, row_ptr, 4);
//             memcpy(&id, row_ptr + 4, 4);
//             cout << indent << "  (Age: " << age << ", ID: " << id << ")" << endl;
//         }
//     } else {
//         // Internal Node entry: [ChildPID (4b)] [Key (4b)] [PK (4b)] = 12 bytes
//         int internal_entry_size = 4 + index_size + pk_size; 
        
//         for (int i = 0; i <= h.keys_count; i++) {
//             int child_pid;
//             int child_off = sizeof(Header) + (i * internal_entry_size);
//             memcpy(&child_pid, (char*)&pg + child_off, 4);

//             if (child_pid > 0) {
//                 // If it's not the last child, print the pivot key that follows it
//                 if (i < h.keys_count) {
//                     int pivot_age;
//                     int pk;
//                     memcpy(&pivot_age, (char*)&pg + child_off + 4, 4);
//                     memcpy(&pk, (char*)&pg + child_off + 8, 4);
//                     cout << indent << "  -- Pivot: " << pivot_age << "   pk: " << pk<<" --" << endl;
//                 }
//                 print_tree_recursive(child_pid, level + 1);
//             }
//         }
//     }
// }

// // In Index.h / Index.cpp
// void Index::display_tree() {
//     cout << "\n******* B+ TREE STRUCTURE DUMP *******" << endl;
//     if (tree.tmd.root_page_id == 0) {
//         cout << "Tree is empty." << endl;
//         return;
//     }
//     print_tree_recursive(tree.tmd.root_page_id, 0);
//     cout << "**************************************\n" << endl;
// }

// int main() {
//     Disk_Manager dm; Table_Metadata tmd;
//     string s1 = "stress_main";
//     string s2 = "stress_idx";
//     dm.create_file(s1);
//     BplusTree main_tree(dm, tmd, datatype::int32, 4, true);
//     int rs = 28;
//     main_tree.create_tree(rs);

//     Disk_Manager dm2; Table_Metadata tmd2;
//     dm2.create_file(s2);
//     Index age_idx(dm2, tmd2, datatype::int32, 4, 4);
//     age_idx.create_index();

//     cout << "--- Phase 1: Heavy Insertion (500 Rows) ---" << endl;
//     for (int i = 1; i <= 500; i++) {
//         int id = i;
//         int age = 20 + (i % 31); // Ages 20 to 50
//         char row[28]; memset(row, 0, 28);
//         memcpy(row, &id, 4);
//         memcpy(row + 4, &age, 4);
//         main_tree.insert_row(row, 28, 0);

//         char idx[8];
//         memcpy(idx, &age, 4);
//         memcpy(idx + 4, &id, 4);
//         age_idx.insert_index(idx);
//     }

//     // age_idx.print_level_order();

//     cout << "--- Phase 2: Range Verification (Age 30) ---" << endl;
//     int target = 32;

//     int count1 = 0;
//     int count2 = 0;
//     int count3 = 0;
//     age_idx.find_all_pks(&target, datatype::int32, [&](const void* pk) {
//         count1++;
//     });
//     cout << "Found " << count1 << " records for Age = 32." << endl;
//     age_idx.find_all_pks_forward(&target, datatype::int32, [&](const void* pk) {
//         count2++;
//     });
//     cout << "Found " << count2 << " records for Age >= 32." << endl;
//     age_idx.find_all_pks_backward(&target, datatype::int32, [&](const void* pk) {
//         count3++;
//     });
//     cout << "Found " << count3 << " records for Age <= 32." << endl;

//     cout << "--- Phase 3: Bulk Deletion (All Even IDs) ---" << endl;
//     for (int i = 2; i <= 500; i += 2) {
//         int id = i;
//         int age = 20 + (i % 31);
//         char idx[8];
//         memcpy(idx, &age, 4);
//         memcpy(idx + 4, &id, 4);
        
//         age_idx.delete_index(idx); // Test tie-breaker deletion
//     }

//     age_idx.print_level_order();

//     cout << "--- Phase 4: Upper/Lower Bound Consistency ---" << endl;
//     int test_age = 44;
//     int lb_page = age_idx.search_lower_bound(&test_age, datatype::int32);
//     int ub_page = age_idx.search_upper_bound(&test_age, datatype::int32);
//     cout << "Lower Bound Page: " << lb_page << " | Upper Bound Page: " << ub_page << endl;

//     return 0;
// }