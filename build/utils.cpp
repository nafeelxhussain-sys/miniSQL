#include <iostream>
#include <string>
using namespace std;

string to_upper(string s) {
    for (int i = 0; i < s.size(); i++)
        s[i] = toupper(s[i]);
    return s;
}