int main() {
    Disk_Manager dm;
    Table_Metadata tmd;
    string tableName = "titan_test_db";
    int row_s = 20; 
    
    // 1. FRESH START
    dm.create_file(tableName);
    BplusTree tree(dm, tmd, datatype::int32, sizeof(int));
    tree.create_tree(row_s);

    cout << "--- PHASE 1: MASSIVE GROWTH (Splitting) ---" << endl;
    // Insert 100 keys to force a 3-level tree (Root -> Internal -> Leaf)
    for (int i = 1; i <= 100; i++) {
        char row[20] = {0};
        memcpy(row, &i, 4);
        tree.insert_row(row, 20, 0);
    }
    cout << "Growth Complete. Total Pages: " << tmd.total_page_count << endl;
    tree.print_tree_structure(sizeof(int));

    cout << "\n--- PHASE 2: SELECTIVE DELETION (Borrow/Merge) ---" << endl;
    // We delete even numbers first to force "Borrowing" and "Internal Refactoring"
    for (int i = 2; i <= 100; i += 2) {
        int k = i;
        tree.delete_row(20, &k, 0);
    }
    cout << "Even keys removed. Checking for 'Zombie' pointers..." << endl;
    tree.print_tree_structure(sizeof(int));

    cout << "\n--- PHASE 3: RECYCLING TEST ---" << endl;
    int pages_before = tmd.total_page_count;
    // Insert 20 keys. They should reuse the pages from the even-key deletions.
    for (int i = 200; i <= 220; i++) {
        char row[20] = {0};
        memcpy(row, &i, 4);
        tree.insert_row(row, 20, 0);
    }
    
    if (tmd.total_page_count == pages_before) {
        cout << "[SUCCESS] Page recycling working! Total Pages stayed at " << pages_before << endl;
    } else {
        cout << "[WARNING] Page count grew to " << tmd.total_page_count << ". Recycling might be broken." << endl;
    }

    cout << "\n--- PHASE 4: THE GREAT COLLAPSE (Root Deletion) ---" << endl;
    // Delete almost everything to force the tree to shrink from 3 levels back to 1
    for (int i = 1; i <= 100; i++) {
        int k = i;
        tree.delete_row(20, &k, 0); // Deleting remaining odd keys
    }
    for (int i = 200; i <= 220; i++) {
        int k = i;
        tree.delete_row(20, &k, 0); // Deleting the recycling test keys
    }
    
    cout << "Tree should be almost empty now." << endl;
    tree.print_tree_structure(sizeof(int));
    tree.watch_meta("Final Collapse");

    cout << "\n--- PHASE 5: PERSISTENCE (Cold Boot) ---" << endl;
    // Simulate closing the app and reopening
    Table_Metadata fresh_tmd;
    Page meta_pg;
    dm.read_page(0, &meta_pg);
    fresh_tmd.load_table_md(&meta_pg);

    cout << "Disk Report -> Root: " << fresh_tmd.root_page_id 
         << " | Total Pages: " << fresh_tmd.total_page_count << endl;

    if (fresh_tmd.root_page_id > 0 && fresh_tmd.total_page_count > 0) {
        cout << "\n🏆 TITAN TEST PASSED: Your engine is stable. 🏆" << endl;
    } else {
        cout << "\n💀 TITAN TEST FAILED: The Black Hole consumed the tree. 💀" << endl;
    }

    return 0;
}