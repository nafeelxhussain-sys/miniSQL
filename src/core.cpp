#include<bits/stdc++.h>
using namespace std;

enum datatype{
    int32=0,
    text=1,
    bool8=2
};

class schema{
    public:
        string table_name;
        int row_size;
        int page_size;
        int num_of_cols;
        int* col_offset;
        string *column_name;
        datatype *dtypes;
    
        schema() {
        col_offset = nullptr;
        column_name = nullptr;
        dtypes = nullptr;
        }

        ~schema(){
            delete[] col_offset;
            delete[] column_name;
            delete[] dtypes;
        }

        void free_arrays(){
            delete[] col_offset;
            delete[] column_name;
            delete[] dtypes;

            col_offset = nullptr;
            column_name = nullptr;
            dtypes = nullptr;
        }

        void load_schema(string db_name,string table_name){
            string file_name = "..\\data\\" + db_name + '_' + table_name + ".schema";
            ifstream in(file_name,ios::binary);
            unsigned char buffer[512]={'\0'};

            if(!in.is_open()){
            cout << "Failed to open file!" << endl;
            return;
            }

            in.seekg(0, ios::end);
            size_t size = in.tellg();
            in.seekg(0, ios::beg);

            in.read((char*)buffer, size);

            //initialise the properties of schema
            this->table_name=table_name;

            uint8_t temp8;
            uint16_t temp16;
            uint32_t temp32;
    
            memcpy(&temp8,buffer,1);
            num_of_cols=temp8;

            free_arrays();

            //initialise the pointers
            col_offset=new int[num_of_cols];
            column_name=new string[num_of_cols];
            dtypes=new datatype[num_of_cols];
            
            memcpy(&temp16,buffer+1,2);
            row_size=temp16;
            
            memcpy(&temp16,buffer+3,2);
            page_size=temp16;
            
            //data about the cols
            int k=0;
            int i=6+table_name.size();
            col_offset[0]=0;
            while(k<num_of_cols){
                memcpy(&temp8,buffer +i,1);
                i++;
                
                string name(reinterpret_cast<const char*>(buffer + i), temp8);
                column_name[k] = name;
                i += temp8;

                memcpy(&temp8,buffer+i,1);
                dtypes[k]=(datatype)temp8;
                i++;
                
                if(k+1<num_of_cols){
                    memcpy(&temp16,buffer+i,2);
                    col_offset[k+1]=temp16+col_offset[k];
                    i+=2;
                }
                
                k++;
            }
        }

        void create_schema_file(string db_name,string table_name,int num_of_cols,string *name,int *size,datatype* type){
            //create a .schema file in disk
            string file_name = "..\\data\\" + db_name + '_' + table_name + ".schema";
            ofstream out(file_name,ios::binary | ios::trunc);
            unsigned char buffer[512]={'\0'};

            if(!out.is_open()){
            cout << "Failed to open file!" << endl;
            return;
            }

            //basic information about table
            memcpy(buffer,&num_of_cols,1);
       
            uint16_t row_size = 0 ;
            for(int i=0;i<num_of_cols;i++) row_size+=size[i];
            memcpy(buffer+1,&row_size,2);
              
            uint16_t page_size = 4096;
            memcpy(buffer+3,&page_size,2);
            
            uint8_t tb_name_len = table_name.length();
            memcpy(buffer+5,&tb_name_len,1);

            memcpy(buffer+6,table_name.c_str(),table_name.length());


            //insert information about coloumn
            int i = 6 + table_name.length();
            for(int k=0;k<num_of_cols;k++){
                const uint8_t  len = name[k].length();
                memcpy(buffer+i,&len,1);
                // i++;

                memcpy(buffer+i+1,name[k].c_str(),len);
                i+=len+1;

                memcpy(buffer+i,&type[k],1);
                i+=1;

                memcpy(buffer+i,&size[k],2);
                i+=2;
            }

            out.write((char*)buffer,i);
            out.close(); 
        }

        void print_schema(){

            cout<<table_name<<endl;
            cout<<row_size<<endl<<page_size<<endl<<num_of_cols<<endl;;

            for(int i=0;i<num_of_cols;i++){
                cout<<column_name[i]<<" "<<(datatype)dtypes[i]<<"("<<col_offset[i]<<")"<<endl;
            }
            
        }


        int getColumnOffset(int colIndex){}	
        int getColumnSize(int colIndex){}	
        datatype getColumnType(int colIndex){}
};

class buffer{
    public:
        int size;
        unsigned char* row_buffer;

    buffer(){
        this->size=4096;
        row_buffer = new unsigned char[size];
    }

    bool verify(schema &s,string *data,int size_of_data){
        if(size_of_data % s.num_of_cols!=0) return false;

        int rows=size_of_data/s.num_of_cols;

        for(int r=0; r<rows;r++){
            for(int c=0;c<s.num_of_cols;c++){
                int index = r*s.num_of_cols +c;
                string test = data[index];
                int text_size=0;

                switch(s.dtypes[c]){
                    case 0://int32
                    try{
                        int value = stoi(test);
                    }catch(...){
                        return false;
                    }break;


                    case 1://text
                    text_size = (c+1==s.num_of_cols) ? s.row_size - s.col_offset[c]:
                    s.col_offset[c+1]-s.col_offset[c];
                    if(test.size()>text_size ) return false;
                    break;

                    case 2://bool
                    if(test!="1" && test!="0") return false;
                    if(test.empty()) return false;

                }
            }
        }

        return true;
    }

    void convert_and_write(const string &value,datatype dt,int position,int limit){
        if(dt==int32){
            uint32_t x = stoi(value);
            memcpy(row_buffer+position,&x,4);
            return;
        }

        if(dt==bool8){
            uint8_t x=0;
            if(value=="1") x=1;
            memcpy(row_buffer+position,&x,1);
            return;
        }

        if(dt==text){
            memcpy(row_buffer+position,value.c_str(),value.size());
            memset(row_buffer + position + value.size(), 0, limit - value.size());
            return;
        }
    }
    

    void fill_buffer(schema &s,string* data,int size_of_data){
        int current_row=0;
        int total_rows=size_of_data/s.num_of_cols;
        this->size=s.row_size * total_rows;
        char nul='\0';

        while(current_row < total_rows){
            int current_row_start = current_row*s.row_size;
            int current_col=0;

            while(current_col < s.num_of_cols){
                int current_col_start = current_row_start + s.col_offset[current_col];
                int index = current_row*s.num_of_cols + current_col;
                int limit = (current_col+1==s.num_of_cols)? s.row_size - s.col_offset[current_col] : 
                s.col_offset[current_col+1]-s.col_offset[current_col] ;

                convert_and_write(data[index],s.dtypes[current_col],current_col_start,limit);
                
                current_col++;
            }

            current_row++;
        }
    }

    void read_buffer(string path){
        ifstream out(path,ios::binary);

        out.seekg(0, ios::end);
        size_t s = out.tellg();
        out.seekg(0, ios::beg);

        out.read((char*) row_buffer,s);
        this->size=s;
    }

    void print_buffer(schema &s){
        int rows = size / s.row_size;

        for(int i=0;i<rows;i++){//i is the number of row
            const unsigned char* current_row_start = row_buffer + (i * s.row_size);
            for(int k=0;k<s.num_of_cols ;k++){//k is the column
                datatype type = s.dtypes[k];
                const unsigned char* col_data_ptr = current_row_start + s.col_offset[k];

                switch(type){
                    case int32: 
                    uint32_t x;
                    memcpy(&x,col_data_ptr,4);
                    cout<<x<<" ";
                    break;

                    case text:{
                    int text_length = s.col_offset[k+1] - s.col_offset[k];
                    for(int x=0;x<text_length;x++){
                    cout <<col_data_ptr[x];
                    }cout<<" ";
                    break;}

                    case bool8:

                    if((col_data_ptr[0]&1)==1){
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

class catalog{
    static const int MAX_TABLES = 50;

    string db_name;
    string table_names[MAX_TABLES];
    int coulumn_counts[MAX_TABLES];
    int table_count;

    void clear() {
    table_count = 0;
    for(int i=0;i<MAX_TABLES;i++){
        table_names[i] = "";
        coulumn_counts[i] = 0;
    }
    }

    public:
    catalog(){
    table_count = 0;
    for(int i=0;i<MAX_TABLES;i++){
        table_names[i] = "";
        coulumn_counts[i] = 0;}
    }


    bool db_exists(string path){
        ifstream out(path,ios::binary);
        return out.is_open();
    }

    void save_catalog(string db_name){
        //create path for file
        string file_name="..\\data\\" + db_name +  ".catalog";
        this->db_name=db_name;
        
        //check if db exists
        if(db_exists(file_name)){
            cout<<"catalog already exists"<<endl;
            return;
        }

        //create a file
        ofstream out(file_name, ios::binary);

        uint32_t x = 0 ;
        out.write((char*)&x,sizeof(x));

        out.close();
    }

    void load_catalog(string db_name){
        //create path for file
        string file_name="..\\data\\" + db_name +  ".catalog";

        //check if db exists
        if(!db_exists(file_name)){
            cout<<"catalog doesnt' exists"<<endl;
            return;
        }

        //clear
        clear();

        //calculate the size of file
        ifstream in(file_name,ios::binary);
        in.seekg(0,ios::end);
        int size_catalog_data = in.tellg();
        in.seekg(0,ios::beg);

        //store file in buffer
        unsigned char *buffer = new unsigned char[size_catalog_data];
        in.read((char*)buffer,size_catalog_data);
        in.close();

        //get num of tables
        memcpy(&table_count,buffer,sizeof(int));
        this->db_name=db_name;

        //use loop to get the data of tables
        int start = 4;
        for(int i=0;i<table_count;i++){
            uint8_t x; 
            memcpy(&x,buffer+start,sizeof(uint8_t));
            start++;

            table_names[i] = string((char*)(buffer + start), x);
            start+=x;

            memcpy(&coulumn_counts[i],buffer+start,2);
            start+=2;
        }

        delete[] buffer;
    }

    void add_table(string table_name, int coulumn_count){
        //create path for file
        string file_name="..\\data\\" + this->db_name +  ".catalog";

        //check if db exists
        if(!db_exists(file_name)){
            cout<<"catalog doesnt' exists"<<endl;
            return;
        }
        //check if table exists
        if(has_table(table_name)){
            cout<<"table already exists"<<endl;
            return ;
        }

        //calculate the size of file 
        ifstream in(file_name,ios::binary);
        in.seekg(0,ios::end);
        int size_catalog_data = in.tellg();
        in.seekg(0,ios::beg);
        
        //size of table data
        int size_table_data = 3 + table_name.size();
        
        //use a buffer to store the data of disk and the data of to be updated in disk
        unsigned char *buffer = new unsigned char[size_catalog_data + size_table_data];

        //copy the file into buffer1
        in.read((char*)buffer,size_catalog_data);
        in.close();

        //add table data to buffer
        uint8_t x = table_name.size();
        memcpy(buffer + size_catalog_data,&x,1);

        memcpy(buffer+size_catalog_data +1,table_name.c_str(),x);

        uint16_t y=coulumn_count;
        memcpy(buffer+size_catalog_data + 1 + x,&y,2);


        //update the members of catalog
        table_names[table_count]=table_name;
        coulumn_counts[table_count]=coulumn_count;
        table_count++;


        //update table count in buffer
        memcpy(buffer,&table_count,sizeof(table_count));
        
        
        ofstream out(file_name,ios::binary);
        out.write((char*)buffer,size_catalog_data + size_table_data);
        out.close();
        delete[] buffer;

        return;
    }

    bool has_table(string table_name){
        for(int i=0;i<table_count;i++){
        if(table_names[i] == table_name) return true;
        }
        return false;
    }

    void print_catalog(){
        for(int i=0;i<table_count;i++){
            cout<<table_names[i]<<"   " <<coulumn_counts[i]<<endl;
        }
    }
};

class database{
    public:
    string db_name;

    database(string name){
        db_name=name;

        catalog c;
        if (!c.db_exists(db_name + ".catalog"))
        c.save_catalog(db_name);
    }

    bool table_exists(string tb_name){
        string file_name="..\\data\\" + this->db_name + '_' + tb_name + ".tbl";
        ifstream f(file_name);
        bool exists=f.is_open();
        f.close();
        return exists;
    }
    bool schema_exists(string tb_name){
        string file_name="..\\data\\" + this->db_name + '_' + tb_name + ".schema";
        ifstream f(file_name);
        bool exists=f.is_open();
        f.close();
        return exists;
    }

   
    
    void create_table(string table_name, int num_of_cols, string *name, int *size, datatype* type){
        string file_name="..\\data\\" + this->db_name + '_' + table_name + ".tbl";

        //check if table exists
        if(table_exists(table_name)){
            cout<<"table already exists"<<endl;
            return;
        }

        //update catalog
        catalog c;
        c.load_catalog(db_name);
        c.add_table(table_name,num_of_cols);

        //create schema file
        schema s;
        s.create_schema_file(this->db_name,table_name,num_of_cols,name,size,type);

        ofstream out(file_name);
        out.close();

        cout<<"table created"<<endl;
        return;
    }

    void insert_into_table(string table_name,string* data,int size_of_data){
        string file_name="..\\data\\" + this->db_name + '_' + table_name + ".tbl";

        //check if table exists
        if(!table_exists(table_name) || !schema_exists(table_name)){
            cout<<"table or schema doesn't exists"<<endl;
            return;
        }
        
        //open the file
        ofstream out(file_name,ios::binary | ios::app);
        
        
        // reads schema 
        schema s;
        s.load_schema(this->db_name,table_name);
        
        //verify schema and data
        buffer b;
        if(!b.verify(s,data,size_of_data)){
            cout<<"invalid data"<<endl;
            return;
        }
        
        //generate buffer
        b.fill_buffer(s,data,size_of_data);
        
        // disk write
        out.write((char*)b.row_buffer,b.size);
        
        out.close();
        
        cout<<"data isnerted"<<endl;
        return;
    }

    void select_from_table(string table_name){
        string file_name="..\\data\\" + this->db_name + '_' + table_name + ".tbl";

        //check if table exists
        if(!table_exists(table_name) && !schema_exists(table_name)){
            cout<<"table or schema doesn't exists"<<endl;
            return;
        }

        //open the file
        ifstream out(file_name,ios::binary);

        // reads schema 
        schema s;
        s.load_schema(this->db_name,table_name);

        //generate buffer
        buffer b;
        b.read_buffer(file_name);

        //print the data
        b.print_buffer(s);

        out.close();
        return;
    }
};

int main(){
    // // --- Define schema ---
    // int size[] = {10, 11, 4, 1}; // column widths (bytes)
    // string name[] = {"name", "enrollment", "marks", "pass"};
    // datatype t[] = {text, text, int32, bool8};

    // // --- Define data ---
    // string data[] = {
    //     "nafeel",    "2023BCSE080", "66", "1",
    //     "raafid",    "2023BCSE030", "90", "1",
    //     "irfan",     "2023BCSE068", "22", "0",
    //     "long",     "2023BCSE099", "88", "1" 
    // };

    // // --- Create table ---
    // database d("test");
    // d.create_table("students", 4, name, size, t);

    // // --- Insert ---
    // d.insert_into_table("students", data, 16);

    // // --- Select ---
    // cout << "\n=== TABLE CONTENTS ===\n";
    // d.select_from_table("students");
}