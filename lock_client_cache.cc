// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"
#include <unistd.h>

void release_user::dorelease(lock_protocol::lockid_t lid) {
    std::cout << " calling flush " << lid << std::endl;
    //ec->flush(lid);
}

release_user::release_user(extent_client *ec) {
    this->ec = ec;
}

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  rpcs *rlsrpc = new rpcs(0);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

  const char *hname;
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlsrpc->port();
  id = host.str();
  pthread_mutex_init(&mutex, NULL);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
    int r;
    lock_protocol::status ret;
    pthread_mutex_t *mutexId;
    pthread_cond_t *acqCondId;
    pthread_cond_t *relCondId;
    pthread_cond_t *retCondId;

    pthread_mutex_lock(&mutex);
    std::cout << id << " acquiring for " << lid << std::endl;
    if (mutexes.find(lid) == mutexes.end()) {
        pthread_mutex_t m;
        pthread_mutex_init(&m, NULL);
        mutexes[lid] = m;
        pthread_cond_t acqConds;
        pthread_cond_init(&acqConds, NULL);
        acquireConds[lid] = acqConds;
        pthread_cond_t relConds;
        pthread_cond_init(&relConds, NULL);
        releaseConds[lid] = relConds;
        pthread_cond_t retConds;
        pthread_cond_init(&retConds, NULL);
        retryConds[lid] = retConds;
    }
    mutexId = &mutexes[lid];
    acqCondId = &acquireConds[lid];
    relCondId = &releaseConds[lid];
    retCondId = &retryConds[lid];
    pthread_mutex_unlock(&mutex);

start:
    pthread_mutex_lock(mutexId);
    if (lockStatus.find(lid) == lockStatus.end() || lockStatus[lid] == NONE) {
        lockStatus[lid] = ACQUIRING;
        std::cout << id << " calling acquire" << std::endl;
        pthread_mutex_unlock(mutexId);

        ret = cl->call(lock_protocol::acquire, lid, id, r);

        pthread_mutex_lock(mutexId);
        std::cout << id << " done calling acquire" << std::endl;
        if (ret == lock_protocol::RETRY) {
            std::cout << id << " RETRY, start waiting" << std::endl;
            std::set<lock_protocol::lockid_t>::iterator it;
            if ((it = lockedIds.find(lid)) == lockedIds.end())
                pthread_cond_wait(retCondId, mutexId);
            lockStatus[lid] = NONE;
            pthread_mutex_unlock(mutexId);

            pthread_mutex_lock(&mutex);
            if (lockedIds.find(lid) != lockedIds.end())
                lockedIds.erase(lockedIds.find(lid));
            pthread_mutex_unlock(&mutex);
            goto start;
        } else if (ret == lock_protocol::OK) {
            std::cout << id << " OK!" << std::endl;
            lockStatus[lid] = LOCKED;
            pthread_cond_broadcast(acqCondId);
            pthread_mutex_unlock(mutexId);
        }
    } else if (lockStatus[lid] == FREE) {
        std::cout << id << " lock is here" << std::endl;
        lockStatus[lid] = LOCKED;
        pthread_mutex_unlock(mutexId);
    } else if (lockStatus[lid] == ACQUIRING) {
        std::cout << id << " acquiring, I'll wait" << std::endl;
        pthread_cond_wait(acqCondId, mutexId);
        pthread_mutex_unlock(mutexId);
        goto start;
    } else if (lockStatus[lid] == RELEASING) {
        std::cout << id << " releasing, I'll wait" << std::endl;
        pthread_cond_wait(relCondId, mutexId);
        pthread_mutex_unlock(mutexId);
        goto start;
    } else {
        std::cout << id << " wtf? the status is: " << lockStatus[lid] << std::endl;
        pthread_mutex_unlock(mutexId);
        sleep(1);
        goto start;
    }
  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
    int r;
    lock_protocol::status ret;
    pthread_mutex_t *mutexId = &mutexes[lid];
    pthread_cond_t *relCondId = &releaseConds[lid];
    lockStatus[lid] = FREE; 

    std::set<lock_protocol::lockid_t>::iterator it;
    pthread_mutex_lock(mutexId);
    if ((it = revokedIds.find(lid)) != revokedIds.end()) {
        std::cout << id << " calling release" << std::endl;
        lu->dorelease(lid);
        lockStatus[lid] = RELEASING;
        pthread_mutex_unlock(mutexId);

        ret = cl->call(lock_protocol::release, lid, id, r);

        pthread_mutex_lock(mutexId);
        std::cout << id << " released" << std::endl;
        if (ret == lock_protocol::OK) {
            lockStatus[lid] = NONE;
            pthread_cond_broadcast(relCondId);
        }
        else
            std::cout << id << " cannot release, it's weird" << std::endl;
        pthread_mutex_unlock(mutexId);

        pthread_mutex_lock(&mutex);
        if (revokedIds.find(lid) != revokedIds.end())
            revokedIds.erase(revokedIds.find(lid));
        pthread_mutex_unlock(&mutex);
    } else {
        pthread_mutex_unlock(mutexId);
    }
  return ret;

}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  int ret = rlock_protocol::OK;
  pthread_mutex_lock(&mutex);
  std::cout << id << " revoke received for " << lid << std::endl;
  pthread_mutex_unlock(&mutex);
    if (lockStatus.find(lid) == lockStatus.end() || lockStatus[lid] == NONE)
        std::cout << "unexpected situation, revoking a non-existed lock" << std::endl;
    else {
        pthread_mutex_lock(&mutex);
        revokedIds.insert(lid);
        if (lockStatus[lid] == FREE) {
            std::cout << id << " calling lease " << lid << std::endl;
            pthread_mutex_unlock(&mutex);
            release(lid);
        } else {
            std::cout << id << " " << lockStatus[lid] << " " << lid << std::endl;
            pthread_mutex_unlock(&mutex);
        }
    }
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  int ret = rlock_protocol::OK;
  pthread_cond_t *retCondId = &retryConds[lid];
  pthread_mutex_lock(&mutex);
  std::cout << id << " retry received" << std::endl;
  lockedIds.insert(lid);
  pthread_cond_broadcast(retCondId);
  pthread_mutex_unlock(&mutex);
  return ret;
}



