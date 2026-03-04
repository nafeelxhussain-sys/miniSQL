#pragma once
#include<iostream>
#include "utils.h"
#include "filter.h"
using namespace std;

class operation;
class schema;

enum AccessType{
    pk_point=1,
    sk_point,
    pk_range,
    sk_range,
    full_scan,
    no_scan,
    heap
};

struct AccessPath{
    AccessType type;

    string search_value;
    string col_name;   
    datatype dt;
    
    bool is_range;
    bool forward;
};

class QueryOptimizer {
    private:
    string get_table_name(operation &o);
    bool is_optimization_needed(operation &o);
    void determine_table_index(operation &o, schema &s);
    void traverse_tree(ConditionNode* root, schema &s, AccessPath &acc_path);
    void determine_root(ConditionNode* &root,operation &o);
    bool is_heap(operation &o);

    public:
    AccessPath optimise(operation &o);
};

