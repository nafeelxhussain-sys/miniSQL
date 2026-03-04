#pragma once
#include <iostream>
#include <string>
#include "parser.h"
#include "core.h"
#include "QueryOptimiser.h"
using namespace std;

class executor {
public:
    void execute(operation &o, AccessPath &acc_path);

    void execute_create_heap(create_operation &o);
    void execute_create_cluster(create_operation &o);

    void execute_insert_heap(insert_operation &o);
    void execute_insert_cluster(insert_operation &o);

    void execute_select_heap(select_operation &o);
    void execute_select_pklookup(select_operation &o, AccessPath &acc);
    void execute_select_sklookup(select_operation &o, AccessPath &acc);
    void execute_select_pkrange(select_operation &o, AccessPath &acc);
    void execute_select_skrange(select_operation &o, AccessPath &acc);
    void execute_select_fullscan(select_operation &o, AccessPath &acc);

    void execute_update_heap(update_operation &o);
    void execute_update_pklookup(update_operation &o, AccessPath &acc);
    void execute_update_sklookup(update_operation &o, AccessPath &acc);
    void execute_update_pkrange(update_operation &o, AccessPath &acc);
    void execute_update_skrange(update_operation &o, AccessPath &acc);
    void execute_update_fullscan(update_operation &o, AccessPath &acc);

    void execute_delete_heap(delete_operation &o);
    void execute_delete_pklookup(delete_operation &o, AccessPath &acc);
    void execute_delete_sklookup(delete_operation &o, AccessPath &acc);
    void execute_delete_pkrange(delete_operation &o, AccessPath &acc);
    void execute_delete_skrange(delete_operation &o, AccessPath &acc);
    void execute_delete_fullscan(delete_operation &o, AccessPath &acc);

    void execute_drop_heap(select_operation &o);
    void execute_drop_cluster(select_operation &o, AccessPath &acc);

    void execute_show(select_operation &o);
};
