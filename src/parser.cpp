#include<iostream>
#include<string>
#include<stack>
// #include"core.cpp"
using namespace std;

enum Error_type {
    ERR_NONE,
    ERR_SYNTAX,
    ERR_UNKNOWN_TABLE,
    ERR_UNKNOWN_COLUMN,
    ERR_TYPE_MISMATCH,
    ERR_RUNTIME
};

class DB_error {
public:
    Error_type type;
    string message;

    DB_error() : type(ERR_NONE), message("") {}

    DB_error(Error_type t, const string& msg)
        : type(t), message(msg) {}
    
    bool ok() const {
        return type == ERR_NONE;
    }
};

template <typename T>
T make_error(Error_type type, string msg) {
    T operation{};
    operation.error = DB_error(type, msg);
    return operation;
}

class ConditionNode {
    public:
    bool is_leaf;            // true for column op value, false for AND/OR node
    ConditionNode* left;     // left child
    ConditionNode* right;    // right child
    string operand;          // for leaf: =, >=; for internal: AND/OR
    string column;           // only for leaf
    string value;            // only for leaf

    ConditionNode(){
        left=nullptr;
        right=nullptr;
    }
};

class create_operation{
    public:
    string table_name;
    string column_names[100];
    datatype column_dtypes[100];
    int column_size[100];
    int num_of_col;
    DB_error error;
};

class insert_operation{
    public:
    string table_name;
    string column_data[100];
    int col_data_size;
    DB_error error;
};

class select_operation{
    public:
    string table_name;
    ConditionNode* root;
    DB_error error;
};

class operation{
    public:
    string operation_type;
    DB_error error;
    
    select_operation select;
    insert_operation insert;
    create_operation create;

    // void print(ConditionNode* root) {
    // if (!root) return;

    // // left
    // print(root->left);

    // if(root->is_leaf){
    //     cout<<"leaf " <<root->column << root->operand <<root->value<<endl;
    // }
    // else{
    //     cout<<root->operand<<endl;
    // }

    // print(root->right);
    
    // }
    // void print(){
    //     // cout<<operation_type<<endl;
    //     // cout<<table_name<<endl;
    //     // cout<<col_data_size<<endl;
        
    //     // for(int i = 0 ; i < num_of_col ; i++){
    //     //     cout<<column_names[i]<<"   "<<column_dtypes[i]<<"    "<<column_size[i]<<endl;
    //     // }cout<<endl;

    //     // for(int i = 0 ; i<col_data_size ;i++){
    //     //     cout<<column_data[i]<<endl;
    //     // }
    // }
};

class query_processor{
    #define max_tokens 100
    int token_count=0;
    string tokens[max_tokens];

    public:

    void print(){
        for(int i=0;i<token_count;i++){
            cout<<tokens[i]<<endl;
        }
    }

    string to_upper(string s) {
    for (int i = 0; i < s.size(); i++)
        s[i] = toupper(s[i]);
    return s;
    }

    void tokenizer(string input) {
    int n = input.size();
    int index = 0;
    string temp = "";

    while (index < n) {
        char c = input[index];

        /* ------------------ QUOTED STRING ------------------ */
        if (!temp.empty() && temp[0] == '\'') {
            temp += c;
            index++;

            if (c == '\'') {
                tokens[token_count++] = temp;
                temp = "";
            }
            continue;
        }

        /* ------------------ WHITESPACE ------------------ */
        if (c == ' ') {
            if (!temp.empty()) {
                tokens[token_count++] = temp;
                temp = "";
            }
            index++;
            continue;
        }

        /* ------------------ BRACKETS & COMMA ------------------ */
        if (c == '(' || c == ')' || c == ',') {
            if (!temp.empty()) {
                tokens[token_count++] = temp;
                temp = "";
            }

            tokens[token_count++] = string(1, c);
            index++;
            continue;
        }

        /* ------------------ COMPARISON OPERATORS ------------------ */
        if (c == '>' || c == '<' || c == '=' || c == '!') {
            if (!temp.empty()) {
                tokens[token_count++] = temp;
                temp = "";
            }

            string op;
            op += c;
            index++;

            // handle >=, <=, !=
            if (index < n && input[index] == '=') {
                op += '=';
                index++;
            }

            tokens[token_count++] = op;
            continue;
        }

        /* ------------------ START QUOTE ------------------ */
        if (c == '\'') {
            if (!temp.empty()) {
                tokens[token_count++] = temp;
                temp = "";
            }

            temp += c;
            index++;
            continue;
        }

        /* ------------------ NORMAL CHARACTER ------------------ */
        temp += c;
        index++;
    }

    /* ------------------ LAST TOKEN ------------------ */
    if (!temp.empty()) {
        tokens[token_count++] = temp;
    }
    }

    operation command_router(){
        if(token_count == 0){
            return make_error<operation>(ERR_SYNTAX,"no query to execute");
        }

        operation o;

        if(to_upper(tokens[0])=="SELECT"){
            o.select = parser_select();
            o.operation_type = "SELECT";
        }
        else if(to_upper(tokens[0])=="INSERT"){
            o.insert = parser_insert();
            o.operation_type = "INSERT";
        }
        else if(to_upper(tokens[0])=="CREATE"){
            o.create = parser_create();
            o.operation_type = "CREATE";
        }

        return o;
    }

    create_operation parser_create(){
        // CREATE TABLE <table_name> ( <col_name> <type> <(size)>, ... )

        //use try catch or token count
        create_operation o;
        if((token_count<6) || (to_upper(tokens[0])!="CREATE" || to_upper(tokens[1])!="TABLE" || (tokens[3]!="("))){
            return make_error<create_operation>(ERR_SYNTAX, "invalid syntax");
        }

        o.table_name=tokens[2];

        int index=4;
        o.num_of_col=0;
        
        while(index<token_count){
            o.column_names[o.num_of_col]=tokens[index++];
            
            string tmp_dt;
            if(index<token_count)
            tmp_dt=to_upper(tokens[index++]);

            if(tmp_dt=="INT") {
                o.column_dtypes[o.num_of_col]=int32;
                o.column_size[o.num_of_col]=4;
            }
            else if(tmp_dt=="BOOL"){
                o.column_dtypes[o.num_of_col]=bool8; 
                o.column_size[o.num_of_col]=1;
            }
            else if(tmp_dt=="TEXT") {
                o.column_dtypes[o.num_of_col]=text; 

                if(index<token_count && tokens[index]=="("){
                    index++;
                }else{
                    return make_error<create_operation>(ERR_SYNTAX, "invalid syntax : missing '('");
                }

                o.column_size[o.num_of_col]=stoi(tokens[index++]); 

                if(index<token_count && tokens[index]==")"){
                    index++;
                }else{
                    return make_error<create_operation>(ERR_SYNTAX, "invalid syntax : missing ')'");
                }
            }

            o.num_of_col++;
            
            if(index<token_count && tokens[index]=="," ){
                index++;
            }
            else if(index<token_count && tokens[index]==")"){
                index++;
                if(index<token_count){
                    return make_error<create_operation>(ERR_SYNTAX, "unexpected token : " +tokens[index]);
                }
                break;
            }
            else{
                return make_error<create_operation>(ERR_SYNTAX, "unexpected token : " +tokens[index]);
            }
            
        }

        return o;
    }

    insert_operation parser_insert(){
        // INSERT INTO <table_name> VALUES ( 'value1' , 32 , true ) , ( ... ) , ....

        insert_operation o;
        if((token_count<6) || (to_upper(tokens[0])!="INSERT" || to_upper(tokens[1])!="INTO" || (tokens[3]!="VALUES"))){
            return make_error<insert_operation>(ERR_SYNTAX, "invalid syntax");
        }

        o.table_name=tokens[2];
        o.col_data_size = 0;
        int index=4;
        
        while(index<token_count){
            
            if(index<token_count && tokens[index]=="("){
                index++;
            }else{
                return make_error<insert_operation>(ERR_SYNTAX, "invalid syntax : missing '('");
            }
            
            
            while(index<token_count && tokens[index]!=")"){
                o.column_data[o.col_data_size++] = tokens[index++];

                if(index<token_count && tokens[index]==","){
                    index++;
                }else if(index<token_count && tokens[index]==")"){
                    continue;
                }else{
                    return make_error<insert_operation>(ERR_SYNTAX, "invalid syntax : missing ','");
                }
            }

            if(index<token_count &&tokens[index]==")"){
                index++;
            }else{
                return make_error<insert_operation>(ERR_SYNTAX, "invalid syntax : missing ')'");
            }

            if(index<token_count &&tokens[index]==","){
                index++;
            }else if(index<token_count ){
                return make_error<insert_operation>(ERR_SYNTAX, "unexpected token : " + tokens[index]);
            }
        }

        return o;
    }

    select_operation parser_select(){
        //SELECT * FROM <table_name> WHERE <column_name> >= 34

        select_operation o;
        if((token_count<4) || (to_upper(tokens[0])!="SELECT" || tokens[1]!="*" || (to_upper(tokens[2])!="FROM"))){
            return make_error<select_operation>(ERR_SYNTAX, "invalid syntax");
        }

        o.table_name=tokens[3];
        int index=4;

        if(token_count>4 && to_upper(tokens[index])!="WHERE"){
            return make_error<select_operation>(ERR_SYNTAX, "unexpected token : " + tokens[index]);
        }
        
        int postfix_token_count = token_count-index;
        int postfix_index = 0;
        string postfix_tokens[postfix_token_count];
        stack<string> op;
        index++;

        //build postfix
        while(index<token_count){
            //logical operator
            if(tokens[index]=="("){
                op.push(tokens[index]);
            }
            else if(tokens[index]==")"){
                while(!op.empty() && op.top()!="("){
                    if(precedence(op.top())){
                        postfix_tokens[postfix_index] = op.top();
                        postfix_index++;
                    }
                    op.pop();
                }
                op.pop();
            }
            else if(precedence(tokens[index])){
                if((op.empty())||precedence(op.top()) < precedence(tokens[index])){
                    op.push(tokens[index]);
                }
                else{
                    while((!op.empty())&&(precedence(op.top()) >= precedence(tokens[index])&&op.top()!="(")){
                        postfix_tokens[postfix_index] = op.top();
                        postfix_index++;
                        op.pop();
                    }
                    op.push(tokens[index]);
                }
            }
            else{
                postfix_tokens[postfix_index++]=tokens[index];
            }

            index++;
        }

        while(!op.empty()){
            if(op.top()=="("){
                return make_error<select_operation>(ERR_SYNTAX, "invalid syntax: missing '('");
            }
            postfix_tokens[postfix_index++]=op.top();
            op.pop();
        }

        //build tree
        stack<ConditionNode*> node;
        postfix_token_count = postfix_index;
        postfix_index = 0;

        while(postfix_index < postfix_token_count){
            ConditionNode* temp = new ConditionNode();
            if(precedence(postfix_tokens[postfix_index])){
                //AND OR
                temp->is_leaf= false;
                temp->operand = to_upper(postfix_tokens[postfix_index++]);

                if(node.size()<2){
                    return make_error<select_operation>(ERR_SYNTAX, "invalid syntax");
                }

                temp->right=node.top();
                node.pop();
                temp->left=node.top();
                node.pop();

                node.push(temp);
            }
            else{//expressin age >= 33
                if(postfix_index >= postfix_token_count-2){
                    return make_error<select_operation>(ERR_SYNTAX, "invalid syntax");
                }
                temp->is_leaf= true;

                temp->column = postfix_tokens[postfix_index++];
                temp->operand = postfix_tokens[postfix_index++];
                temp->value = postfix_tokens[postfix_index++];

                if(temp->operand=="=" || temp->operand=="!=" || temp->operand==">=" || temp->operand=="<=" || temp->operand==">" || temp->operand=="<" ){
                    node.push(temp);
                }
                else{
                    return make_error<select_operation>(ERR_SYNTAX, "unexpected token : " + temp->operand);
                }
            }
        }

        o.root = node.top();

        return o;
    }

    int precedence(string op) {
    if (to_upper(op) == "AND") return 2;
    if (to_upper(op) == "OR")  return 1;
    return 0;
}
    
};

// int main(){
    // query p;
    // // p.tokenizer("   INSERT INTO        employees VALUES (    101   , '  Alice  zoo  ',true) ,(102,'machu',false)  ");
    // p.tokenizer("CREATE TABLE table123 (name text(10), marks int  )");
    // p.tokenizer("select * from table112 where (marks >= 45 and gender='f') or city = 'srinagar' and marks<33");
    // p.print();

    // operation o = p.parser_insert();
//     operation o = p.parser_select();
//     o.print(o.root);
// }