// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache::lock_server_cache()
{  
    pthread_mutex_init(&mutex, NULL);
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{

    pthread_mutex_lock(&mutex);
    if (rpccList.find(id) == rpccList.end()) {
        sockaddr_in dstsock;
        make_sockaddr(id.c_str(), &dstsock);
        rpcc *cl = new rpcc(dstsock);
        if (cl->bind() < 0) {
            printf("lock_server_cache revoke: call bind\n");
        }
        rpccList[id] = cl;
        std::cout << "rpcc saved for " << id << std::endl;
    }
    rpcc *cl = rpccList[id];
    lock_protocol::status ret = lock_protocol::OK;
    printf("acquire request from clt %s, lockid %llu\n", id.c_str(), lid);
    if (locks.find(lid) == locks.end()) {
        locks[lid] = id;
        std::cout << "inside acquire, new lock request" << lid << std::endl;
        pthread_mutex_unlock(&mutex);
        return ret;
    } 
    //calling again
    if (locks[lid] == id) {
        std::cout << id << " already got lock because called again or retry is called" << std::endl;
        if (lockList.find(lid) != lockList.end() && lockList[lid].size() != 0) {
            int r;
            std::cout << "call revoke because called again or retry is called " << id << std::endl;
            pthread_mutex_unlock(&mutex);

            cl->call(rlock_protocol::revoke, lid, r);

            pthread_mutex_lock(&mutex);
        }
        pthread_mutex_unlock(&mutex);
        return ret;
    }
    ret = lock_protocol::RETRY;
    if (lockList.find(lid) == lockList.end() || lockList[lid].size() == 0 ) {
        std::set<std::string> set;
        set.insert(id);
        lockList[lid] = set;
        rpcc *cl = rpccList[locks[lid]];
        std::cout << "inside acquire, set insert: " << id << std::endl;
        int r;
        std::cout << "inside acquire, calling revoke " << locks[lid] << std::endl;
        pthread_mutex_unlock(&mutex);

        cl->call(rlock_protocol::revoke, lid, r);

        pthread_mutex_lock(&mutex);
        std::cout << "inside revoke, done revoke calling" << std::endl;
    } else {
        std::set<std::string> *set = &(lockList[lid]);
        if (set->find(id) == set->end()) {
            set->insert(id);
            std::cout << "inside acquire, set inserted: " << id << std::endl;
        }
    }
    pthread_mutex_unlock(&mutex);
    return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
    pthread_mutex_lock(&mutex);
    lock_protocol::status ret = lock_protocol::OK;
    printf(" release request from clt %s, lockid %llu\n", id.c_str(), lid);
    if (locks.find(lid) == locks.end()) {
        ret = lock_protocol::NOENT;
        std::cout << id << " no entry in release for " << lid << std::endl;
    } else {
        //Calling again
        if (locks[lid] != id) {
            std::cout << " already released for " << id << std::endl;
            pthread_mutex_unlock(&mutex);
            return ret;
        }
        locks.erase(locks.find(lid));
        if (lockList.find(lid) != lockList.end()) {
            std::set<std::string> *set = &(lockList[lid]);
            if (set->size() != 0) {
                std::set<std::string>::iterator it = set->begin();
                std::string dst = *it;
                locks[lid] = dst;
                set->erase(it);
                std::cout << " inside release, dest to call retry: " << dst << std::endl;
                if (rpccList.find(dst) == rpccList.end())
                    std::cout << "cannot find rpcc " << id << std::endl;
                rpcc *cl = rpccList[dst];
                int r;
                std::cout << " inside release, calling retry for " << lid << std::endl;
                pthread_mutex_unlock(&mutex);

                cl->call(rlock_protocol::retry, lid, r);

                pthread_mutex_lock(&mutex);
                std::cout << " inside release, done retry calling" << std::endl;
            }
        }
    }
    pthread_mutex_unlock(&mutex);
    return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  lock_protocol::status ret = lock_protocol::OK;
  if (lockList.find(lid) == lockList.end() || lockList[lid].size() == 0)
    ret = lock_protocol::NOENT;
 return ret;
}

