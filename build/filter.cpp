#include <iostream>
#include <stack>
#include <cstring>
#include "schema.h"
#include "error.h"
#include "filter.h"
#include "utils.h"
using namespace std;

// ------------------- where_clause -------------------
DB_error where_clause::make_tree(int token_count, int index, string tokens[], ConditionNode*& root) {
    int postfix_token_count = token_count - index;
    int postfix_index = 0;
    string postfix_tokens[postfix_token_count];
    stack<string> op;
    index++;

    // Build postfix
    while (index < token_count) {
        if (tokens[index] == "(") {
            op.push(tokens[index]);
        } else if (tokens[index] == ")") {
            while (!op.empty() && op.top() != "(") {
                if (precedence(op.top())) {
                    postfix_tokens[postfix_index++] = op.top();
                }
                op.pop();
            }
            if (!op.empty()) op.pop();
        } else if (precedence(tokens[index])) {
            if ((op.empty()) || precedence(op.top()) < precedence(tokens[index])) {
                op.push(tokens[index]);
            } else {
                while ((!op.empty()) && (precedence(op.top()) >= precedence(tokens[index]) && op.top() != "(")) {
                    postfix_tokens[postfix_index++] = op.top();
                    op.pop();
                }
                op.push(tokens[index]);
            }
        } else {
            postfix_tokens[postfix_index++] = tokens[index];
        }
        index++;
    }

    while (!op.empty()) {
        if (op.top() == "(") return DB_error(ERR_SYNTAX, "invalid syntax: missing '('");
        postfix_tokens[postfix_index++] = op.top();
        op.pop();
    }

    // Build tree
    stack<ConditionNode*> node;
    postfix_token_count = postfix_index;
    postfix_index = 0;

    while (postfix_index < postfix_token_count) {
        ConditionNode* temp = new ConditionNode();
        if (precedence(postfix_tokens[postfix_index])) {
            temp->is_leaf = false;
            temp->operand = to_upper(postfix_tokens[postfix_index++]);

            if (node.size() < 2){
                delete_(node);
                return DB_error(ERR_SYNTAX, "invalid syntax");
            }

            temp->right = node.top(); node.pop();
            temp->left = node.top(); node.pop();
            node.push(temp);
        } else {
            if (postfix_index >= postfix_token_count - 2){
                delete_(node);
                return DB_error(ERR_SYNTAX, "invalid syntax");
            }

            temp->is_leaf = true;
            temp->column = postfix_tokens[postfix_index++];
            temp->operand = postfix_tokens[postfix_index++];
            temp->value = postfix_tokens[postfix_index++];

            if (temp->operand == "=" || temp->operand == "!=" || temp->operand == ">=" ||
                temp->operand == "<=" || temp->operand == ">" || temp->operand == "<") {
                node.push(temp);
            } else {
                delete_(node);
                return DB_error(ERR_SYNTAX, "invalid syntax");
            }
        }
    }
    if(node.size()!=1){
        return DB_error(ERR_SYNTAX, "missing tokens");
    }
    root = node.top();
    return DB_error(ERR_NONE, "");
}

void where_clause :: delete_tree(ConditionNode* root){
    //base case
    if(root == nullptr){
        return;
    }

    //left subtree
    delete_tree(root->left);

    //right subtree
    delete_tree(root->right);

    //current node
    delete root;
}

void where_clause :: delete_(stack<ConditionNode*> &node){
    while(!node.empty()){
        delete_tree(node.top());
        node.pop();
    }
}

bool where_clause :: evaluvate_tree(ConditionNode* root, schema &s,const unsigned char* row_buffer){
    //base case
    if(root == nullptr){
        return true;
    }

    bool left=false, right=false, center=false;

    if(root->is_leaf){
        //leaf node
        int column_index = s.getColumnIndex(root->column);

        if(column_index == -1){
            throw DB_error(ERR_UNKNOWN_COLUMN, "unknown column : " + root->column);
        }

        int offset = s.getColumnOffset(column_index);
        int size = s.getColumnSize(column_index);
        datatype dt = s.getColumnType(column_index);

        switch(dt){
            case 0:{ //int32
                uint32_t value, threshold;
                try{
                    value = static_cast<uint32_t>(stoul(root->value));
                }
                catch(...){
                    throw DB_error(ERR_TYPE_MISMATCH, "expected INT : " + root->value);
                }

                memcpy(&threshold, row_buffer + offset, sizeof(uint32_t));

                center = condition_evaluate<uint32_t>(value,threshold, root->operand);
                break;
            }

            case 1:{ //text
                string value = root->value;

                string threshold(reinterpret_cast<const char*>(row_buffer + offset), size);

                size_t end = threshold.find('\0');   // find first null
                if (end != string::npos) threshold.resize(end);  // truncate at null

                center = condition_evaluate<string>(value,threshold, root->operand);
                break;
            }

            case 2:{//bool
                bool value =false;

                if(to_upper(root->value)=="TRUE") value = true;
                else if(to_upper(root->value)=="FALSE") value = false;
                else{
                    throw DB_error(ERR_TYPE_MISMATCH, "expected BOOL : " + root->value);
                }

                uint8_t temp;
                memcpy(&temp, row_buffer+offset,sizeof(uint8_t));
                
                bool threshold = temp;

                center = condition_evaluate<bool>(value,threshold, root->operand);
                break;
            }
        
        }
    }else{
        //AND OR

        left = evaluvate_tree(root->left,s,row_buffer);
        right = evaluvate_tree(root->right,s,row_buffer);

        if(root->operand == "AND")
        center = left && right;

        else if(root->operand == "OR")
        center = left || right;

    }

    return center;
}

int SetClause ::find_column(string col_name){
    int index = -1;

    for(int i = 0 ; i <set_cols ; i++){
        if(to_upper(col_name) == to_upper(column_names[i])) return i;
    }

    return index;
}

