#include<bits/stdc++.h>
using namespace std;

class buffer{
    public:
    int size;
    int num_of_col;
    unsigned char* row_buffer;
    int* col_offset;

    buffer(int s,int* cols,int n){
        this->num_of_col=n;
        this->size=s;
        row_buffer = new unsigned char[size];
        col_offset = cols;
    }

    void fill_buffer(){
        // name 10 bytes: 0-9
        // Marks 4 bytes: 10-13
        // address 20 bytes: 14-33
        // gender 1 bytes: 34-34
        //size =35 col=4

        unsigned char a[10]={'n','a','f','e','e','l','l','i','l','y'};
        uint32_t b=4294967295;
        unsigned char c[20]={'h','o','m','E','\0'};
        unsigned char d[1]={'m'};
        memcpy(row_buffer+0,a,10);
        memcpy(row_buffer+10,&b,4);
        memcpy(row_buffer+14,c,20);
        memcpy(row_buffer+34,d,1);
    }

    void print(){
        for(int i=0;i<10;i++){
            cout << row_buffer[i];
        }cout<<" ";
        uint32_t x;
        memcpy(&x,row_buffer+10,4);
        cout<<x<<" ";
        for(int i=14;i<34;i++){
            cout << row_buffer[i];
        }cout<<" ";
        cout<<row_buffer[34]<<endl;
    }
};

class database{
    public:
    string db_name;
    //it is stored in memory will think of putting it in file schema later
    database(){
        db_name="naff";
    }

    void create_table(string table_name){
        string file_name="..\\data\\" + this->db_name + '_' + table_name + ".tbl";
        ofstream out(file_name);
        out.close();
    }

    void insert_into_table(string table_name,buffer b){
        string file_name="..\\data\\" + this->db_name + '_' + table_name + ".tbl";
        ofstream out(file_name,ios::binary | ios::app);


        if(!out.is_open()){return;}//if not open, file doesnt exist 

        // reads schema and generates buffer
        // disk write

        out.write((char*)b.row_buffer,b.size);

        out.close();
    }

    void select_from_table(string table_name,buffer b){
        string file_name="..\\data\\" + this->db_name + '_' + table_name + ".tbl";
        ifstream out(file_name,ios::binary );

        if(!out.is_open()){return;}//if not open, file doesnt exist

        // reads schema and generates buffer
        // disk read
        
        out.read((char*)b.row_buffer,b.size);
        out.close();
    }
};

int main(){
    // ofstream out("sample.tbl");
    
    database d;
    // d.create_table("tall"); 

    //write a table
    // int col[4]={0,10,14,34};
    // buffer b(35,col,4);
    // b.fill_buffer();
    // // b.print();
    // d.insert_into_table("tall",b);
    // cout<<"written";
    
    
    // read a table
    int col2[4]={0,10,14,34};
    buffer b2(35,col2,4);
    d.select_from_table("tall",b2);
    b2.print();



}