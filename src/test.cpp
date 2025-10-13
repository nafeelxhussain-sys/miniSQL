#include<bits/stdc++.h>
using namespace std;

int main(){
    // char *t;
    // t=new char[3];
    // char in[4];
    // cin >> in;
    // in[5]='\0';
    // strncpy(t,in,5);
    // for(int i=0;i<5;i++){
    //     if(t[i]=='\0') break;
    //     cout<<t[i];
    // }

    unsigned char c[4];
    unsigned char m[4];
    cin>>c;
    cout<<c<<"  "<<sizeof(c)<<endl;;

    memcpy(m,c,4);
    cout<<c;
}