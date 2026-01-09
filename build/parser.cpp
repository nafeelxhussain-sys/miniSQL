#include "parser.h"
#include "utils.h"

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

// ------------------- utility -------------------


int precedence(string op) {
    if (to_upper(op) == "AND") return 2;
    if (to_upper(op) == "OR") return 1;
    return 0;
}

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

            if (node.size() < 2)
                return DB_error(ERR_SYNTAX, "invalid syntax");

            temp->right = node.top(); node.pop();
            temp->left = node.top(); node.pop();
            node.push(temp);
        } else {
            if (postfix_index >= postfix_token_count - 2)
                return DB_error(ERR_SYNTAX, "invalid syntax");

            temp->is_leaf = true;
            temp->column = postfix_tokens[postfix_index++];
            temp->operand = postfix_tokens[postfix_index++];
            temp->value = postfix_tokens[postfix_index++];

            if (temp->operand == "=" || temp->operand == "!=" || temp->operand == ">=" ||
                temp->operand == "<=" || temp->operand == ">" || temp->operand == "<") {
                node.push(temp);
            } else {
                return DB_error(ERR_SYNTAX, "unexpected token : " + temp->operand);
            }
        }
    }
    root = node.top();
    return DB_error(ERR_NONE, "");
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
    } else if(to_upper(tokens[0])=="SHOW"){
        o.select = parser_show(); o.operation_type = "SHOW";
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

    if(token_count ==4) return o;
    
    where_clause w;
    ConditionNode* node;
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


