#include<iostream>
#include<cstring>
#include "QueryOptimiser.h"
#include "schema.h"
#include "parser.h"
using namespace std;

bool QueryOptimizer::is_optimization_needed(operation &o){
    string opType = o.operation_type;

    if((opType=="UPDATE" || opType=="DELETE" || opType=="SELECT")&&(o.clustered_storage))
    return true;

    return false;
}

string QueryOptimizer::get_table_name(operation &o){
    string opType = o.operation_type;

    if(opType=="CREATE") return o.create.table_name;
    if(opType=="INSERT") return o.insert.table_name;
    if(opType=="UPDATE") return o.update.table_name;
    if(opType=="DELETE") return o.delete_.table_name;
    if(opType=="SELECT") return o.select.table_name;

    return "";
}

void QueryOptimizer::determine_table_index(operation &o, schema &s){
    // schema s;
    // string tb_name= get_table_name(o);
    // s.load_schema("main" , tb_name);

    o.clustered_storage = s.is_clustered;

    string opType = o.operation_type;

    if(opType=="UPDATE") {
        for(int i = 0 ; i<s.num_of_cols ; i++)
        o.update.column_index[i] = s.col_index[i];
    }

    if(opType=="DELETE") {
        for(int i = 0 ; i<s.num_of_cols ; i++)
        o.delete_.column_index[i] = s.col_index[i];
    }

    if(opType=="SELECT") {
        for(int i = 0 ; i<s.num_of_cols ; i++)
        o.select.column_index[i] = s.col_index[i];
    }
}

void QueryOptimizer::traverse_tree(ConditionNode* root, schema &s, AccessPath &acc_path){
    //post order traversal

    if(root==nullptr) return;

    traverse_tree(root->left,s,acc_path);
    traverse_tree(root->right,s,acc_path);


    if(root->is_leaf){
        int index_type = s.getColumnIndexType(root->column); // returns 0, 1, 2
        bool point = root->operand == "=";

        int path = 0;

        if(index_type==0) path = 5;
        else if(index_type==1 && point) path = 1;
        else if(index_type==1 && !point) path = 3;
        else if(index_type==2 && point) path = 2;
        else if(index_type==2 && !point) path = 4;


        //if this is better than already prev path
        if(acc_path.type > path){
            acc_path.type = (AccessType)path;
            acc_path.is_range = !point;
            int col_no = s.getColumnIndex(root->column);
            acc_path.dt = s.getColumnType(col_no);
            acc_path.search_value = root->value;
            acc_path.operation = root->operand;
        }
    }
}

void QueryOptimizer::determine_root(ConditionNode* &root,operation &o){
    if(o.operation_type=="UPDATE") root = o.update.root;
    if(o.operation_type=="DELETE") root = o.delete_.root;
    if(o.operation_type=="SELECT") root = o.select.root;
}

bool QueryOptimizer::is_heap(operation &o){
    for(int i = 0 ; i<o.create.num_of_col ; i++)
        if(1==o.create.column_index[i]) 
            return false;

    return true;
}

AccessPath QueryOptimizer::optimise(operation &o){
    AccessPath acc_path;

    string table_name = get_table_name(o);

    if(is_heap(o)&&((to_upper(o.operation_type)=="CREATE")||(to_upper(o.operation_type)=="DROP"))){ 
        acc_path.type = heap; return acc_path;
    }
    else if(!is_heap(o)&&((to_upper(o.operation_type)=="CREATE")||(to_upper(o.operation_type)=="DROP"))){
        acc_path.type = no_scan; return acc_path;
    }

    //load schema
    schema s;
    s.load_schema("main", table_name);

    if(s.is_clustered) acc_path.type = full_scan;
    else {acc_path.type = heap; return acc_path;}

    determine_table_index(o,s);

    if(!is_optimization_needed(o)) return acc_path;

    ConditionNode* root = nullptr;
    determine_root(root,o);

    if(root==nullptr) return acc_path;
    
    traverse_tree(root,s,acc_path);

    return acc_path;
}