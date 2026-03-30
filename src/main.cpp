#include <chrono>
#include "core.h"
#include "parser.h"
#include "executor.h"
#include "utils.h"
#include "schema.h"
#include "filter.h"
using namespace std;


int main(){
    cout<<"Welcome to MiniSQL"<<endl;
    cout<<"Type exit to close"<<endl<<endl;

    string query = "";

    query_processor q;
    executor Exe;
    QueryOptimizer queryOpt;

    while(true){
        query = read_query();
        if(to_upper(query)=="EXIT") break;
        if(query=="") continue;

        cout<<endl;
        
        auto start = std::chrono::high_resolution_clock::now();   // start timer

        q.tokenizer(query);
        operation o = q.command_router();
        AccessPath ap = queryOpt.optimise(o);
        Exe.execute(o,ap);

        auto end = std::chrono::high_resolution_clock::now();     // end timer
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        cout << "\nQuery completed in " << duration.count()<< " ms" << endl<<endl;
    }
}
