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

// int main() {

//     query_processor q;
//     executor Exe;
//     QueryOptimizer queryOpt;

//     const int TOTAL = 1000000;

//     auto start = chrono::high_resolution_clock::now();

//     string query = "CREATE TABLE users (id INT key,name TEXT(14) index,age INT index , active bool )";

//     q.tokenizer(query);
//     operation o = q.command_router();
//     AccessPath ap = queryOpt.optimise(o);
//     Exe.execute(o, ap);

//     int i = 1;

//     while(i <= TOTAL) {

//     string query = "INSERT INTO users VALUES ";
//     int lim = i + 1000;

//     while(i < lim && i <= TOTAL) {

//         int id = i;
//         int age = rand() % 80 + 10;
//         int act = rand() % 2;
//         string active = (act==0) ? "false" : "true";
//         string name = "user" + to_string(i);

//         query += "(" + to_string(id) + ", '" +
//                  name + "', " +
//                  to_string(age) + ", " +
//                  active + "),";

//         i++;
//     }

//     query.pop_back();

//     q.tokenizer(query);
//     operation o = q.command_router();
//     AccessPath ap = queryOpt.optimise(o);
//     Exe.execute(o, ap);

//     if(i % 10000 == 0)
//     cout << i/1000 << "k rows inserted\n";
//     }

//     auto end = chrono::high_resolution_clock::now();
//     auto duration = chrono::duration_cast<chrono::seconds>(end - start);

//     cout << "\nInserted " << TOTAL << " rows in "
//          << duration.count() << " seconds\n";
// }