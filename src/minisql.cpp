#include "core.h"
#include "parser.h"
#include "executor.h"
using namespace std;

int main(){
    cout<<"Welcome to MiniSQL"<<endl;
    cout<<"Type exit to close"<<endl<<endl;

    string query = "";

    database db("main");
    query_processor q;
    executor exe;

    while(true){
        getline(cin,query);

        if(to_upper(query)=="EXIT") break;

        q.tokenizer(query);
        operation o = q.command_router();

        exe.execute(o,db);
    }
}