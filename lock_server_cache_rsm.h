#ifndef lock_server_cache_rsm_h
#define lock_server_cache_rsm_h

#include <string>

#include "lock_protocol.h"
#include "rpc.h"
#include "rsm_state_transfer.h"
#include "rsm.h"
#include <set>

class lock_server_cache_rsm : public rsm_state_transfer {
 private:
  int nacquire;
  class rsm *rsm;
  rpcc* client;
  pthread_mutex_t mutex;
  pthread_cond_t retryCond;
  pthread_cond_t revokeCond;
  std::map<lock_protocol::lockid_t, std::set<std::string> > waitLists;
  std::map<lock_protocol::lockid_t, std::string> locks;
  std::map<std::string, std::map<lock_protocol::lockid_t, lock_protocol::xid_t> > seqs;
  lock_protocol::lockid_t lid;
  lock_protocol::xid_t xid;
 public:
  lock_server_cache_rsm(class rsm *rsm = 0);
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  void revoker();
  void retryer();
  std::string marshal_state();
  void unmarshal_state(std::string state);
  int acquire(lock_protocol::lockid_t, std::string id, 
	      lock_protocol::xid_t, int &);
  int release(lock_protocol::lockid_t, std::string id, lock_protocol::xid_t,
	      int &);
};

#endif
