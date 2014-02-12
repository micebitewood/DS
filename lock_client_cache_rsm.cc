// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache_rsm.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"

#include "rsm_client.h"


static void *
releasethread(void *x)
{
  lock_client_cache_rsm *cc = (lock_client_cache_rsm *) x;
  cc->releaser();
  return 0;
}

int lock_client_cache_rsm::last_port = 0;

lock_client_cache_rsm::lock_client_cache_rsm(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache_rsm::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache_rsm::retry_handler);
  xid = 0;
  // You fill this in Step Two, Lab 7
  // - Create rsmc, and use the object to do RPC 
  //   calls instead of the rpcc object of lock_client
  rsmc = new rsm_client(xdst);
  pthread_t th;
  int r = pthread_create(&th, NULL, &releasethread, (void *) this);
  VERIFY (r == 0);
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&cond, NULL);
}


void
lock_client_cache_rsm::releaser()
{
  pthread_mutex_lock(&mutex);
  while(true) {
    pthread_cond_wait(&cond, &mutex);
    int r;
    lockStatus[lid] = RELEASING;
    std::cout << id << " start calling release" << std::endl;
    pthread_mutex_unlock(&mutex);
    //int ret = cl->call(lock_protocol::release, lid, id, xid, r);
    int ret = rsmc->call(lock_protocol::release, lid, id, xid, r);
    pthread_mutex_lock(&mutex);
    if (ret == lock_protocol::OK) {
        lockStatus[lid] = NONE;
        std::cout << id << " released!" << std::endl;
        pthread_cond_broadcast(relCondId);
    }
    else {
      lockStatus[lid] = FREE;
        std::cout << id << " cannot release, it's weird" << std::endl;
      }
  }
  pthread_mutex_unlock(&mutex);
  // This method should be a continuous loop, waiting to be notified of
  // freed locks that have been revoked by the server, so that it can
  // send a release RPC.

}


lock_protocol::status
lock_client_cache_rsm::acquire(lock_protocol::lockid_t lid)
{
    int r;
    lock_protocol::status ret;
    ScopedLock ml(&mutex);
    struct timespec ts;
    struct timeval tp;
    std::cout << id << " acquiring for " << lid << std::endl;
    if (acquireConds.find(lid) == acquireConds.end()) {
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
    pthread_cond_t *acqCondId = &acquireConds[lid];
    relCondId = &releaseConds[lid];
    pthread_cond_t *retCondId = &retryConds[lid];

start:
    if (lockStatus.find(lid) == lockStatus.end() || lockStatus[lid] == NONE) {
        lockStatus[lid] = ACQUIRING;
        std::cout << id << " calling acquire" << std::endl;
        xid++;
        pthread_mutex_unlock(&mutex);

        //ret = cl->call(lock_protocol::acquire, lid, id, xid, r);
        ret = rsmc->call(lock_protocol::acquire, lid, id, xid, r);

        pthread_mutex_lock(&mutex);
        std::cout << id << " done calling acquire" << std::endl;
        if (ret == lock_protocol::RETRY) {
            std::cout << id << " RETRY, start waiting" << std::endl;
            std::set<lock_protocol::lockid_t>::iterator it;
            //if ((it = lockedIds.find(lid)) == lockedIds.end())
            //  pthread_cond_wait(retCondId, &mutex);
            while (true) {
              if ((it = lockedIds.find(lid)) != lockedIds.end()) {
                lockedIds.erase(it);
                lockStatus[lid] = NONE;
                goto start;
              }
              gettimeofday(&tp,NULL);
              ts.tv_sec  = tp.tv_sec;
              ts.tv_nsec = tp.tv_usec * 1000;
              ts.tv_sec += 3;
              pthread_cond_timedwait(retCondId, &mutex, &ts);
              if ((it = lockedIds.find(lid)) != lockedIds.end()) {
                lockedIds.erase(it);
                lockStatus[lid] = NONE;
                goto start;
              }
              pthread_mutex_unlock(&mutex);
              ret = rsmc->call(lock_protocol::acquire, lid, id, xid, r);
              pthread_mutex_lock(&mutex);
              if (ret == lock_protocol::OK) {
                lockStatus[lid] = NONE;
                goto start;
              }
            }
        } else if (ret == lock_protocol::OK) {
            std::cout << id << " OK!" << std::endl;
            lockStatus[lid] = LOCKED;
            pthread_cond_broadcast(acqCondId);
        }
    } else if (lockStatus[lid] == FREE) {
        std::cout << id << " lock is here" << std::endl;
        lockStatus[lid] = LOCKED;
    } else if (lockStatus[lid] == ACQUIRING) {
        std::cout << id << " acquiring, I'll wait" << std::endl;
        pthread_cond_wait(acqCondId, &mutex);
        goto start;
    } else if (lockStatus[lid] == RELEASING) {
        std::cout << id << " releasing, I'll wait" << std::endl;
        pthread_cond_wait(relCondId, &mutex);
        goto start;
    } else if (lockStatus[lid] == LOCKED) {
      std::cout << id << " locked" << std::endl;
      pthread_mutex_unlock(&mutex);
      sleep(1);
      pthread_mutex_lock(&mutex);
      goto start;
    } else {
      std::cout << id << " wtf? the status is: " << lockStatus[lid] << std::endl;
      pthread_mutex_unlock(&mutex);
      sleep(1);
      pthread_mutex_lock(&mutex);
      goto start;
    }
  
  return ret;
}

lock_protocol::status
lock_client_cache_rsm::release(lock_protocol::lockid_t lid)
{
    ScopedLock ml(&mutex);
    lock_protocol::status ret = lock_protocol::OK;
    std::set<lock_protocol::lockid_t>::iterator it;
    if ((it = revokedIds.find(lid)) != revokedIds.end()) {
        revokedIds.erase(it);
        relCondId = &releaseConds[lid];
        std::cout << id << " calling release" << std::endl;
        this->lid = lid;
        pthread_cond_signal(&cond);
        //if (lu != NULL)
        //  lu->dorelease(lid);
    } else {
      lockStatus[lid] = FREE; 
    }
  return ret;

}


rlock_protocol::status
lock_client_cache_rsm::revoke_handler(lock_protocol::lockid_t lid, 
			          lock_protocol::xid_t xid, int &)
{
  ScopedLock ml(&mutex);
  int ret = rlock_protocol::OK;
  std::cout << id << " revoke received for " << lid << std::endl;
    if (lockStatus.find(lid) == lockStatus.end() || lockStatus[lid] == NONE)
        std::cout << "unexpected situation, revoking a non-existed lock" << std::endl;
    else {
        revokedIds.insert(lid);
        if (lockStatus[lid] == FREE) {
            std::cout << id << " calling lease " << lid << std::endl;
            pthread_mutex_unlock(&mutex);
            release(lid);
            pthread_mutex_lock(&mutex);
        } else {
            std::cout << id << " " << lockStatus[lid] << " " << lid << " LOCKED: " << LOCKED << std::endl;
        }
    }
  return ret;
}

rlock_protocol::status
lock_client_cache_rsm::retry_handler(lock_protocol::lockid_t lid, 
			         lock_protocol::xid_t xid, int &)
{
  ScopedLock ml(&mutex);
  int ret = rlock_protocol::OK;
  std::cout << id << " retry received" << std::endl;
  if (lockStatus[lid] == ACQUIRING) {
    std::cout << id << " retry" << std::endl;
    lockedIds.insert(lid);
    pthread_cond_t *retCondId = &retryConds[lid];
    pthread_cond_broadcast(retCondId);
  }
  return ret;
}


