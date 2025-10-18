    // read a table
    int col2[6]={0,10,14,34,35,36};
    datatype dt[5]={text,int32,text,text,bool8};
    buffer b2(180,col2,5,dt);
    d.select_from_table("files",b2);
    b2.print();