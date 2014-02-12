// the caching lock server implementation

#include "lock_server_cache_rsm.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


static void *
revokethread(void *x)
{
  lock_server_cache_rsm *sc = (lock_server_cache_rsm *) x;
  sc->revoker();
  return 0;
}

static void *
retrythread(void *x)
{
  lock_server_cache_rsm *sc = (lock_server_cache_rsm *) x;
  sc->retryer();
  return 0;
}

lock_server_cache_rsm::lock_server_cache_rsm(class rsm *_rsm) 
  : rsm (_rsm)
{
  pthread_t th;
  int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  VERIFY (r == 0);
  r = pthread_create(&th, NULL, &retrythread, (void *) this);
  VERIFY (r == 0);
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&retryCond, NULL);
  pthread_cond_init(&revokeCond, NULL);
  rsm->set_state_transfer(this);
}

void
lock_server_cache_rsm::revoker()
{
  ScopedLock ml(&mutex);
  while(true) {
    pthread_cond_wait(&revokeCond, &mutex);
    int r;
    pthread_mutex_unlock(&mutex);
    client->call(rlock_protocol::revoke, lid, xid, r);
    pthread_mutex_lock(&mutex);
  }
  // This method should be a continuous loop, that sends revoke
  // messages to lock holders whenever another client wants the
  // same lock
}


void
lock_server_cache_rsm::retryer()
{
  ScopedLock ml(&mutex);
  while(true) {
    pthread_cond_wait(&retryCond, &mutex);
    int r;
    pthread_mutex_unlock(&mutex);
    client->call(rlock_protocol::retry, lid, xid, r);
    pthread_mutex_lock(&mutex);
  }
  // This method should be a continuous loop, waiting for locks
  // to be released and then sending retry messages to those who
  // are waiting for it.
}


int lock_server_cache_rsm::acquire(lock_protocol::lockid_t lid, std::string id, 
             lock_protocol::xid_t xid, int &)
{
  /*
  if (!rsm->amiprimary()) {
    printf("acquire request from clt %s, lockid %llu\n", id.c_str(), lid);
    std::cout << "not primary" << std::endl;
    return lock_protocol::RPCERR;
  }
  */
  ScopedLock ml(&mutex);

    lock_protocol::status ret = lock_protocol::OK;
    printf("acquire request from clt %s, lockid %llu\n", id.c_str(), lid);
    if (locks.find(lid) == locks.end()) {
        locks[lid] = id;
        std::map<lock_protocol::lockid_t, lock_protocol::xid_t> seq;
        seq[lid] = xid;
        seqs[id] = seq;
        std::cout << "inside acquire, new lock request " << lid << std::endl;
        return ret;
    } 
    std::cout << "current id: " << locks[lid] << ", request id: " << id << std::endl;
    //calling again
    if (locks[lid] == id) {
        std::cout << id << " already got lock because called again or retry is called" << std::endl;
        if (waitLists.find(lid) != waitLists.end() && waitLists[lid].size() != 0) {
            std::cout << "call revoke because called again or retry is called " << id << std::endl;
            if (rsm->amiprimary()) {
              std::cout << "i am primary" << std::endl;
              this->lid = lid;
              this->xid = xid;
              pthread_cond_signal(&revokeCond);
            }
        }
        return ret;
    }
    ret = lock_protocol::RETRY;
    if (waitLists.find(lid) == waitLists.end() || waitLists[lid].size() == 0 ) {
        std::set<std::string> set;
        set.insert(id);
        seqs[id][lid] = xid;
        waitLists[lid] = set;
        handle h(locks[lid]);
        client = h.safebind();
        std::cout << "inside acquire, set insert: " << id << std::endl;
        std::cout << "inside acquire, calling revoke " << locks[lid] << std::endl;
        this->lid = lid;
        this->xid = seqs[locks[lid]][lid];
        if (rsm->amiprimary()) {
          pthread_cond_signal(&revokeCond);
        }
    } else {
        std::set<std::string> *set = &(waitLists[lid]);
        if (set->find(id) == set->end()) {
            set->insert(id);
            seqs[id][lid] = xid;
            std::cout << "inside acquire, set inserted: " << id << std::endl;
        }
    }
    return ret;
}

int 
lock_server_cache_rsm::release(lock_protocol::lockid_t lid, std::string id, 
         lock_protocol::xid_t xid, int &r)
{
  ScopedLock ml(&mutex);
    lock_protocol::status ret = lock_protocol::OK;
    printf(" release request from clt %s, lockid %llu\n", id.c_str(), lid);
    if (locks.find(lid) == locks.end()) {
        ret = lock_protocol::NOENT;
        std::cout << id << " no entry in release for " << lid << std::endl;
    } else {
      std::cout << "current id: " << locks[lid] << ", request id: " << id << std::endl;
        //Calling again
        if (locks[lid] != id) {
            std::cout << " already released for " << id << std::endl;
            if (rsm->amiprimary()) {
              handle h(locks[lid]);
              client = h.safebind();
              this->lid = lid;
              this->xid = seqs[locks[lid]][lid];
              pthread_cond_signal(&retryCond);
            }
            return ret;
        }
        seqs[id][lid] = xid;
        locks.erase(locks.find(lid));
        if (waitLists.find(lid) != waitLists.end()) {
            std::set<std::string> *set = &(waitLists[lid]);
            if (set->size() != 0) {
                std::set<std::string>::iterator it = set->begin();
                std::string dst = *it;
                locks[lid] = dst;
                set->erase(it);
                std::cout << " inside release, dest to call retry: " << dst << std::endl;
                handle h(dst);
                client = h.safebind();
                this->lid = lid;
                this->xid = seqs[dst][lid];
                std::cout << " inside release, calling retry for " << lid << std::endl;
                if (rsm->amiprimary()) {
                  pthread_cond_signal(&retryCond);
                }
            }
        }
    }
    return ret;
}

std::string
lock_server_cache_rsm::marshal_state()
{
  
  ScopedLock ml(&mutex);
  marshall rep;
  rep << locks.size();
  std::map<lock_protocol::lockid_t, std::string>::iterator locks_it;
  for (locks_it = locks.begin(); locks_it != locks.end(); locks_it++) {
    lock_protocol::lockid_t lid = locks_it->first;
    std::string id = locks[lid];
    rep << lid;
    rep << id;
  }
  rep << waitLists.size();
  std::map<lock_protocol::lockid_t, std::set<std::string> >::iterator waitList_it;
  for (waitList_it = waitLists.begin(); waitList_it != waitLists.end(); waitList_it++) {
    lock_protocol::lockid_t lid = waitList_it->first;
    rep << lid;
    std::set<std::string> set = waitLists[lid];
    rep << set.size();
    std::set<std::string>::iterator set_it;
    for (set_it = set.begin(); set_it != set.end(); set_it++) {
      rep << *set_it;
    }
  }
  rep << seqs.size();
  std::map<std::string, std::map<lock_protocol::lockid_t, lock_protocol::xid_t> >::iterator seqs_it;
  for (seqs_it = seqs.begin(); seqs_it != seqs.end(); seqs_it++) {
    std::string id = seqs_it->first;
    std::map<lock_protocol::lockid_t, lock_protocol::xid_t> seq = seqs[id];
    std::map<lock_protocol::lockid_t, lock_protocol::xid_t>::iterator seq_it;
    rep << seq.size();
    for (seq_it = seq.begin(); seq_it != seq.end(); seq_it++) {
      lock_protocol::lockid_t lid = seq_it->first;
      lock_protocol::xid_t xid = seq[lid];
      rep << lid;
      rep << xid;
    }
  }
  return rep.str();
  
  //std::string str;
  //return str;
}

void
lock_server_cache_rsm::unmarshal_state(std::string state)
{
  
  ScopedLock ml(&mutex);
  unmarshall rep(state);
  unsigned int locks_size;
  rep >> locks_size;
  for (unsigned int i = 0; i < locks_size; i++) {
    lock_protocol::lockid_t lid;
    std::string id;
    rep >> lid;
    rep >> id;
    locks[lid] = id;
  }
  unsigned int waitList_size;
  rep >> waitList_size;
  for (unsigned int i = 0; i < waitList_size; i++) {
    lock_protocol::lockid_t lid;
    rep >> lid;
    unsigned int set_size;
    rep >> set_size;
    std::set<std::string> set;
    for (unsigned int j = 0; j < set_size; j++) {
      std::string id;
      rep >> id;
      set.insert(id);
    }
    waitLists[lid] = set;
  }
  unsigned int seqs_size;
  rep >> seqs_size;
  for (unsigned int i = 0; i < seqs_size; i++) {
    std::string id;
    std::map<lock_protocol::lockid_t, lock_protocol::xid_t> seq;
    unsigned int seq_size;
    rep >> seq_size;
    for (unsigned int j = 0; j < seq_size; j++) {
      lock_protocol::lockid_t lid;
      lock_protocol::xid_t xid;
      rep >> lid;
      rep >> xid;
      seq[lid] = xid;
    }
    seqs[id] = seq;
  }
  
}

lock_protocol::status
lock_server_cache_rsm::stat(lock_protocol::lockid_t lid, int &r)
{
  printf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

