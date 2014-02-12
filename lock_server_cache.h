#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>

#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
#include <set>

struct params {
    lock_protocol::lockid_t lockId;
    std::string cid;
};

class lock_server_cache {
 private:
  int nacquire;
 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
  std::map<lock_protocol::lockid_t, std::set<std::string> > lockList;
  std::map<lock_protocol::lockid_t, std::string> locks;
    pthread_mutex_t mutex;
    std::map<std::string, rpcc*> rpccList;
};

#endif
