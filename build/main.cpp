#include "core.h"
#include "parser.h"
#include "executor.h"
#include "utils.h"
#include "schema.h"
#include "filter.h"
using namespace std;

string read_query() {
    string line, query;
    bool in_quote = false;

    while (true) {
        if (!getline(cin, line)) return "";  // EOF / Ctrl+D

        if(query.empty() && to_upper(line) == "EXIT"){
            return "EXIT";
        }

        if (line.empty() && !query.empty() && query.find(';') == string::npos) {
            if(query == " "){
                return "";
            }

            cout << "ERROR: missing ';'"<<endl<<endl;
            return "";
        }

        for (char c : line) {
            if (c == '\'') in_quote = !in_quote;
            query += c;
        }
        query += ' ';

        if (!in_quote && query.find(';') != string :: npos)
            break;
    }

    // remove trailing semicolon
    size_t pos = query.rfind(';');
    if (pos != string::npos)
        query.erase(pos, 1);

    return query;
}


int main(){
    cout<<"Welcome to MiniSQL"<<endl;
    cout<<"Type exit to close"<<endl<<endl;

    string query = "";

    database db("main");
    query_processor q;
    executor exe;

    while(true){
        query = read_query();

        if(to_upper(query)=="EXIT") break;

        if(query=="") continue;

        cout<<endl;
        
        q.tokenizer(query);
        operation o = q.command_router();
        
        exe.execute(o,db);
        cout<<endl;
    }
}