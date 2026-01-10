#pragma once
#include <iostream>
#include <string>
#include <stack>
#include "schema.h"
#include "error.h"
using namespace std;

class ConditionNode {
public:
    bool is_leaf;
    ConditionNode* left;
    ConditionNode* right;
    string operand;
    string column;
    string value;

    ConditionNode() : is_leaf(false), left(nullptr), right(nullptr) {}
};

class where_clause {
public:
    DB_error make_tree(int token_count, int index, string tokens[], ConditionNode*& root);
    void delete_tree(ConditionNode* root);
    void delete_(stack<ConditionNode*> &node);
    template <typename T>
    bool condition_evaluate(T value, T threshold,  string operand);
    bool evaluvate_tree(ConditionNode* root, schema &s,const unsigned char* row_buffer);
};

template <typename T>
    bool where_clause::condition_evaluate(T value, T threshold,  string operand){
    // VALUE OPERATOR THRESHOLD

    if (operand == "=")  return value == threshold;
    if (operand == "!=") return value != threshold;
    if (operand == ">")  return value < threshold;
    if (operand == "<")  return value > threshold;
    if (operand == ">=") return value <= threshold;
    if (operand == "<=") return value >= threshold;

    return false;
}