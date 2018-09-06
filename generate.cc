#include <iostream>
#include <cassert>
#include <string>
#include <cstdlib>
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#define KEY_LIMIT 500000
#define READ_LIMIT 100000
using namespace std;

int main()
{
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing=true;
    options.error_if_exists=false;
    options.filter_policy=leveldb::NewBloomFilterPolicy(10);
    leveldb::Status status=leveldb::DB::Open(options,"./test",&db);
    assert(status.ok());
    printf("Load...\n");
    for(int i=0;i<=KEY_LIMIT;i+=1)
    {

        string value=to_string(i);
        string key=to_string(i);
        status=db->Put(leveldb::WriteOptions(),key,value);
        //assert(status.ok());
    }
    string value;
    string count;
    printf("Test read...\n");
    int count_read_error=0;
    for(int i=0;i<=READ_LIMIT;i+=1)
    {
        int random_num=rand()%KEY_LIMIT;
        string value=to_string(random_num);
        string key=to_string(random_num);
        string readvalue;
        status=db->Get(leveldb::ReadOptions(),key,&readvalue,&count);        
        if(value.compare(readvalue)!=0)
            count_read_error++;
        //assert(status.ok());
    }
    if(count_read_error==0)
        printf("Read test completed\n");
    else
        printf("Read test error: %d\n",count_read_error);
    
    printf("Update...\n");
    for(int i=0;i<KEY_LIMIT;i+=5)
    {
        string value="new"+to_string(i);
        string key=to_string(i);
        status=db->Put(leveldb::WriteOptions(),key,value);
    }

    printf("Test read...\n");
    count_read_error=0;
    for(int i=0;i<=READ_LIMIT;i+=1)
    {
        int random_num=rand()%KEY_LIMIT;
        string value=to_string(random_num);
        if(random_num%5==0)
            value="new"+to_string(random_num);
        string key=to_string(random_num);
        string readvalue;
        status=db->Get(leveldb::ReadOptions(),key,&readvalue,&count);        
        if(value.compare(readvalue)!=0)
            count_read_error++;
    }
    if(count_read_error==0)
        printf("Read test completed\n");
    else
        printf("Read test error: %d\n",count_read_error);

    delete db;
    delete options.filter_policy;
}


