#include<iostream>
#include "parser.h"
#include "utils.h"
#include "filter.h"
using namespace std;

// ------------------- select_operation print -------------------
void select_operation::print(ConditionNode* root) {
    if (!root) return;

    print(root->left);

    if (root->is_leaf) {
        cout << "leaf " << root->column << root->operand << root->value << endl;
    } else {
        cout << root->operand << endl;
    }

    print(root->right);
}


// ------------------- query_processor -------------------
void query_processor::print() {
    for (int i = 0; i < token_count; i++)
        cout << tokens[i] << endl;
}

void query_processor::tokenizer(string input) {
    int n = input.size();
    int index = 0;
    token_count = 0;
    string temp = "";

    while (token_count < max_tokens && index < n) {
        char c = input[index];

        // Quoted string
        if (!temp.empty() && temp[0] == '\'') {
            temp += c;
            index++;
            if (c == '\'') { temp.erase(0,1); temp.erase(temp.size()-1,1);tokens[token_count++] = temp; temp = ""; }
            continue;
        }

        else if (c == ' ') {
            if (!temp.empty()) { tokens[token_count++] = temp; temp = ""; }
            index++; continue;
        }

        else if (c == '(' || c == ')' || c == ',') {
            if (!temp.empty()) { tokens[token_count++] = temp; temp = ""; }
            if(token_count<max_tokens){
            tokens[token_count++] = string(1, c);
            index++; continue;}
        }

        else if (c == '>' || c == '<' || c == '=' || c == '!') {
            if (!temp.empty()) { tokens[token_count++] = temp; temp = ""; }
            string op; op += c; index++;
            if (index < n && input[index] == '=') { op += '='; index++; }
            if(token_count<max_tokens)
            tokens[token_count++] = op; continue;
        }

        else if (c == '\'') {
            if (!temp.empty()) { tokens[token_count++] = temp; temp = ""; }
            temp += c; index++; continue;
        }

        temp += c;
        index++;
    }

    if (!temp.empty() && token_count<max_tokens) tokens[token_count++] = temp;

    if(token_count == max_tokens){
        token_count = -1;
    }

}

operation query_processor::command_router() {
    operation o;

    if(token_count == -1){
        o.error = DB_error(ERR_RUNTIME,"too many tokens");
    }
    
    else if (to_upper(tokens[0]) == "SELECT") {
        o.select = parser_select(); o.operation_type = "SELECT";
    } else if (to_upper(tokens[0]) == "INSERT") {
        o.insert = parser_insert(); o.operation_type = "INSERT";
    } else if (to_upper(tokens[0]) == "CREATE") {
        o.create = parser_create(); o.operation_type = "CREATE";
    } else if (to_upper(tokens[0]) == "UPDATE") {
        o.update = parser_update(); o.operation_type = "UPDATE";
    } else if (to_upper(tokens[0]) == "DELETE") {
        o.delete_ = parser_delete(); o.operation_type = "DELETE";
    } else if(to_upper(tokens[0])=="SHOW"){
        o.select = parser_show(); o.operation_type = "SHOW";
    }
     else if(to_upper(tokens[0])=="DROP"){
        o.select = parser_drop(); o.operation_type = "DROP";
    }
    
    return o;
}

// ------------------- parser_create -------------------
create_operation query_processor::parser_create() {
    create_operation o;
    if ((token_count < 6) || (to_upper(tokens[0]) != "CREATE" || to_upper(tokens[1]) != "TABLE" || tokens[3] != "("))
        return make_error<create_operation>(ERR_SYNTAX, "invalid syntax");

    o.table_name = tokens[2];
    int index = 4; o.num_of_col = 0;

    while (index < token_count) {
        o.column_names[o.num_of_col] = tokens[index++];
        string tmp_dt = "";
        if (index < token_count) tmp_dt = to_upper(tokens[index++]);

        if (tmp_dt == "INT") { o.column_dtypes[o.num_of_col] = int32; o.column_size[o.num_of_col] = 4; }
        else if (tmp_dt == "BOOL") { o.column_dtypes[o.num_of_col] = bool8; o.column_size[o.num_of_col] = 1; }
        else if (tmp_dt == "TEXT") {
            o.column_dtypes[o.num_of_col] = text;
            if (index < token_count && tokens[index] == "(") index++;
            else return make_error<create_operation>(ERR_SYNTAX, "invalid syntax : missing '('");
            o.column_size[o.num_of_col] = stoi(tokens[index++]);
            if (index < token_count && tokens[index] == ")") index++;
            else return make_error<create_operation>(ERR_SYNTAX, "invalid syntax : missing ')'");
        }

        o.num_of_col++;
        if (index < token_count && tokens[index] == ",") index++;
        else if (index < token_count && tokens[index] == ")") { index++; if (index < token_count) return make_error<create_operation>(ERR_SYNTAX, "unexpected token : " + tokens[index]); break; }
        else return make_error<create_operation>(ERR_SYNTAX, "unexpected token : " + tokens[index]);
    }
    return o;
}

// ------------------- parser_insert -------------------
insert_operation query_processor::parser_insert() {
    insert_operation o;

    if ((token_count < 6) || (to_upper(tokens[0]) != "INSERT" || to_upper(tokens[1]) != "INTO" || to_upper(tokens[3]) != "VALUES"))
    return make_error<insert_operation>(ERR_SYNTAX, "invalid syntax");
    

    o.table_name = tokens[2]; o.col_data_size = 0;
    int index = 4;
    
    while (index < token_count) {
        if (index < token_count && tokens[index] == "(") index++;
        else return make_error<insert_operation>(ERR_SYNTAX, "invalid syntax : missing '('");
        
        while (index < token_count && tokens[index] != ")") {
            o.column_data[o.col_data_size++] = tokens[index++];
            if (index < token_count && tokens[index] == ",") index++;
            else if (index < token_count && tokens[index] == ")") continue;
            else return make_error<insert_operation>(ERR_SYNTAX, "invalid syntax : missing ','");
        }
        
        if (index < token_count && tokens[index] == ")") index++;
        else return make_error<insert_operation>(ERR_SYNTAX, "invalid syntax : missing ')'");
        
        if (index < token_count && tokens[index] == ",") index++;
        else if (index < token_count) return make_error<insert_operation>(ERR_SYNTAX, "unexpected token : " + tokens[index]);
    }

    return o;
}

// ------------------- parser_select -------------------
select_operation query_processor::parser_select() {
    select_operation o;
    if ((token_count < 4) || (to_upper(tokens[0]) != "SELECT" || tokens[1] != "*" || to_upper(tokens[2]) != "FROM"))
        return make_error<select_operation>(ERR_SYNTAX, "invalid syntax");

    o.table_name = tokens[3];
    int index = 4;

    if (token_count > 4 && to_upper(tokens[index]) != "WHERE")
        return make_error<select_operation>(ERR_SYNTAX, "unexpected token : " + tokens[index]);

    if(token_count ==4) {
        o.root = nullptr;
        return o;
    }
    else if(token_count <=7){
        return make_error<select_operation>(ERR_SYNTAX, "invalid syntax");
    }
    
    where_clause w;
    ConditionNode* node=nullptr;
    DB_error err = w.make_tree(token_count, index, tokens, node);
    o.error = err; o.root = node;

    return o;
}


select_operation query_processor::parser_show(){
    select_operation o;
    DB_error err(ERR_NONE,"");
    o.error =err;
    o.table_name = tokens[1];

    if(token_count!=2){
        return make_error<select_operation>(ERR_SYNTAX, "invalid syntax");
    }

    return o;
}

update_operation query_processor::parser_update(){
    // UPDATE <TABLE_NAME> SET <column_name> = value , ... , WHERE conditon;
    
    update_operation o;
    if ((token_count < 6) || (to_upper(tokens[0]) != "UPDATE"  || to_upper(tokens[2]) != "SET"))
    return make_error<update_operation>(ERR_SYNTAX, "invalid syntax");
    
    o.table_name = tokens[1];
    int index = 3;

    SetClause sc;
    sc.set_cols = 0;
    while(index < token_count){
        if(index + 2 >= token_count){
            return make_error<update_operation>(ERR_SYNTAX, "invalid syntax");
        }
        
        sc.column_names[sc.set_cols] = tokens[index++];
        
        if(tokens[index]!="="){
            return make_error<update_operation>(ERR_SYNTAX, "expected = : " +tokens[index]);
        }index++;
        
        sc.column_values[sc.set_cols] = tokens[index++];
        
        sc.set_cols++;

        if(index<token_count && tokens[index]==","){
            index++;
        }else if(index<token_count &&to_upper(tokens[index])=="WHERE"){
            break;
        }else if(index<token_count){
            return make_error<update_operation>(ERR_SYNTAX, "invalid syntax");
        }
    }

    o.sc=sc;
    if(index==token_count) {o.root = nullptr ;return o;}
    
    
    where_clause w;
    ConditionNode* node=nullptr;
    DB_error err = w.make_tree(token_count, index, tokens, node);
    o.error = err; o.root = node;
    

    return o;
}

delete_operation query_processor::parser_delete(){
    // DELETE FROM <TABLE_NAME>  WHERE conditon;

    delete_operation o;
    if ((token_count < 3) || (to_upper(tokens[0]) != "DELETE"  || to_upper(tokens[1]) != "FROM"))
        return make_error<delete_operation>(ERR_SYNTAX, "invalid syntax");

    o.table_name = tokens[2];
    int index = 3;

    if (token_count > 3 && to_upper(tokens[index]) != "WHERE")
        return make_error<delete_operation>(ERR_SYNTAX, "unexpected token : " + tokens[index]);

    if(token_count == 3){ o.root =nullptr; return o;}
    
    where_clause w;
    ConditionNode* node=nullptr;
    DB_error err = w.make_tree(token_count, index, tokens, node);
    o.error = err; o.root = node;

    return o;
}


select_operation query_processor::parser_drop() {
    select_operation o;
    if ((token_count !=3 ) || (to_upper(tokens[0]) != "DROP" || to_upper(tokens[1]) != "TABLE"))
        return make_error<select_operation>(ERR_SYNTAX, "invalid syntax ");

    o.table_name = tokens[2];

    return o;
}

