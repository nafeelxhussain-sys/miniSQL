#include<bits/stdc++.h>
using namespace std;

enum datatype{
    int32=0,
    text=1,
    bool8=2
};



class buffer{
    public:
        int size;
        int num_of_col;
        unsigned char* row_buffer;
        int* col_offset;
        datatype *dtypes;

    buffer(int s,int* cols,int n,datatype *d){
        this->num_of_col=n;
        this->size=s;
        row_buffer = new unsigned char[size];
        col_offset = cols;
        this->dtypes=d;
    }

    void fill_buffer(){
    // Row offsets
    int row_size = 36;

    // --- Row 1 ---
    unsigned char name1[10] = {'A','L','I','C','E','\0','\0','\0','\0','\0'};
    uint32_t marks1 = 10;
    unsigned char addr1[20] = {'D','E','L','H','I','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0'};
    unsigned char gender1[1] = {'F'};
    unsigned char dis1[1] = {'T'};
    memcpy(row_buffer + 0*row_size + 0, name1, 10);
    memcpy(row_buffer + 0*row_size + 10, &marks1, 4);
    memcpy(row_buffer + 0*row_size + 14, addr1, 20);
    memcpy(row_buffer + 0*row_size + 34, gender1, 1);
    memcpy(row_buffer + 0*row_size + 35, dis1, 1);

    // --- Row 2 ---
    unsigned char name2[10] = {'B','O','B','\0','\0','\0','\0','\0','\0','\0'};
    uint32_t marks2 = 20;
    unsigned char addr2[20] = {'M','U','M','B','A','I','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0'};
    unsigned char gender2[1] = {'M'};
    unsigned char dis2[1] = {'F'};
    memcpy(row_buffer + 1*row_size + 0, name2, 10);
    memcpy(row_buffer + 1*row_size + 10, &marks2, 4);
    memcpy(row_buffer + 1*row_size + 14, addr2, 20);
    memcpy(row_buffer + 1*row_size + 34, gender2, 1);
    memcpy(row_buffer + 1*row_size + 35, dis2, 1);

    // --- Row 3 --- 
    unsigned char name3[10] = {'C','H','A','R','L','I','E','\0','\0','\0'};
    uint32_t marks3 = 30;
    unsigned char addr3[20] = {'C','H','E','N','N','A','I','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0'};
    unsigned char gender3[1] = {'M'};
    unsigned char dis3[1] = {'T'};
    memcpy(row_buffer + 2*row_size + 0, name3, 10);
    memcpy(row_buffer + 2*row_size + 10, &marks3, 4);
    memcpy(row_buffer + 2*row_size + 14, addr3, 20);
    memcpy(row_buffer + 2*row_size + 34, gender3, 1);
    memcpy(row_buffer + 2*row_size + 35, dis3, 1);

    // --- Row 4 ---
    unsigned char name4[10] = {'D','A','V','I','D','\0','\0','\0','\0','\0'};
    uint32_t marks4 = 40;
    unsigned char addr4[20] = {'K','O','L','K','A','T','A','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0'};
    unsigned char gender4[1] = {'M'};
    unsigned char dis4[1] = {'F'};
    memcpy(row_buffer + 3*row_size + 0, name4, 10);
    memcpy(row_buffer + 3*row_size + 10, &marks4, 4);
    memcpy(row_buffer + 3*row_size + 14, addr4, 20);
    memcpy(row_buffer + 3*row_size + 34, gender4, 1);
    memcpy(row_buffer + 3*row_size + 35, dis4, 1);

    // --- Row 5 ---
    unsigned char name5[10] = {'E','M','M','A','\0','\0','\0','\0','\0','\0'};
    uint32_t marks5 = 50;
    unsigned char addr5[20] = {'P','U','N','E','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0'};
    unsigned char gender5[1] = {'F'};
    unsigned char dis5[1] = {'F'};
    memcpy(row_buffer + 4*row_size + 0, name5, 10);
    memcpy(row_buffer + 4*row_size + 10, &marks5, 4);
    memcpy(row_buffer + 4*row_size + 14, addr5, 20);
    memcpy(row_buffer + 4*row_size + 34, gender5, 1);
    memcpy(row_buffer + 4*row_size + 35, dis5, 1);
    }

    void print(){
        int row_size=col_offset[num_of_col];
        int rows = size/row_size;
        for(int i=0;i<rows;i++){//i is the number of row
            const unsigned char* current_row_start = row_buffer + (i * row_size);
            for(int k=0;k<num_of_col ;k++){//k is the column
                datatype type = dtypes[k];
                const unsigned char* col_data_ptr = current_row_start + col_offset[k];

                switch(type){
                    case int32: 
                    uint32_t x;
                    memcpy(&x,col_data_ptr,4);
                    cout<<x<<" ";
                    break;

                    case text:{
                    int text_length = col_offset[k+1] - col_offset[k];
                    for(int x=0;x<text_length;x++){
                    cout <<col_data_ptr[x];
                    }cout<<" ";
                    break;}

                    case bool8:
                    if(col_data_ptr[0]=='T'){
                        cout<<"True"<<" ";
                    }else{
                    cout<<"false"<<" ";
                    }break;
                }
            }
            cout<<endl;
        }
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
 
    database d;
    // d.create_table("files"); 

    // // write a table
    // int col[6]={0,10,14,34,35,36};
    // datatype dt[5]={int32,text,int32,int32,bool8};
    // buffer b(180,col,5,dt);
    // b.fill_buffer();
    // d.insert_into_table("files",b);
    
    
    
    // // read a table
    // int col2[6]={0,10,14,34,35,36};
    // datatype dt[5]={text,int32,text,text,bool8};
    // buffer b2(180,col2,5,dt);
    // d.select_from_table("files",b2);
    // b2.print();
}