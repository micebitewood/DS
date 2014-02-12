// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"
#include <set>
#include "extent_client.h"

// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 5.
class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {};
};

class release_user: public lock_release_user {
private:
    extent_client *ec;
public:
    release_user(extent_client *);
    void dorelease(lock_protocol::lockid_t);
};

class lock_client_cache : public lock_client {
 private:
    enum status {NONE, FREE, LOCKED, ACQUIRING, RELEASING};
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;

  std::set<lock_protocol::lockid_t> lockedIds;
  std::set<lock_protocol::lockid_t> revokedIds;
  std::map<lock_protocol::lockid_t, status> lockStatus;
  std::map<lock_protocol::lockid_t, pthread_mutex_t> mutexes;
  std::map<lock_protocol::lockid_t, pthread_cond_t> acquireConds;
  std::map<lock_protocol::lockid_t, pthread_cond_t> releaseConds;
  std::map<lock_protocol::lockid_t, pthread_cond_t> retryConds;
  std::string id;
  pthread_mutex_t mutex;
 public:
  lock_client_cache(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache() {};
  lock_protocol::status acquire(lock_protocol::lockid_t);
  lock_protocol::status release(lock_protocol::lockid_t);
  rlock_protocol::status revoke_handler(lock_protocol::lockid_t, 
                                        int &);
  rlock_protocol::status retry_handler(lock_protocol::lockid_t, 
                                       int &);
};


#endif
