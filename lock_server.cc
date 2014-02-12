// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <iostream>
#include <pthread.h>

static std::map<lock_protocol::lockid_t, int> states;
static pthread_mutex_t mutex;
static std::map<lock_protocol::lockid_t, pthread_cond_t> conds;

void *handleAcquire(void *arg) {
    requests *request = (requests*) arg;
    lock_protocol::lockid_t lockId = request->lockId;
    int client = request->client;

    pthread_mutex_lock(&mutex);
    if (states.find(lockId) == states.end()) {
        states[lockId] = client;
    }
    else {
        pthread_cond_t *cond;
        if (conds.find(lockId) == conds.end()) {
            pthread_cond_t lockCond;
            pthread_cond_init(&lockCond, NULL);
            conds[lockId] = lockCond;
            cond = &conds[lockId];
        }
        else {
            cond = &conds[lockId];
        }
        pthread_cond_wait(cond, &mutex);
        states[lockId] = client;
    }
    pthread_mutex_unlock(&mutex);
    lock_protocol::status ret = lock_protocol::OK;
    pthread_exit((void *)ret);
}

void *handleRelease(void *arg) {
    requests *request = (requests*) arg;
    lock_protocol::lockid_t lockId = request->lockId;
    int client = request->client;

    lock_protocol::status ret = lock_protocol::OK;
    pthread_mutex_lock(&mutex);
    if (states.find(lockId) == states.end()) {
        ret = lock_protocol::NOENT;
    }
    else if (states[lockId] == client) {
        states.erase(lockId);
        pthread_cond_t *cond = &conds[lockId];
        pthread_cond_signal(cond);
    }
    else {
        ret = lock_protocol::RPCERR;
    }
    pthread_mutex_unlock(&mutex);
    pthread_exit((void *)ret);
}

lock_server::lock_server():
  nacquire (0)
{
    pthread_mutex_init(&lockMutex, NULL);
    states = lockStates;
    mutex = lockMutex;
}

lock_server::~lock_server() {
    pthread_mutex_destroy(&lockMutex);
    std::map<lock_protocol::lockid_t, pthread_cond_t *>::iterator iter;
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  if (states.find(lid) == states.end()) {
    ret = lock_protocol::NOENT;
  }
  printf("stat request from clt %d, lockid %llu\n", clt, lid);
  return ret;
}

lock_protocol::status lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r) {
    requests request;
    request.lockId = lid;
    request.client = clt;

    pthread_t thread;
    int res;
    void *status;

    res = pthread_create(&thread, NULL, &handleAcquire, (void*) &request);
    if (res) {
        printf("ERROR: return code from pthread_create() is %d\n", res);
    }
    res = pthread_join(thread, &status);
    lock_protocol::status ret = (lock_protocol::status ) status;
    printf("acquire request from clt %d, lockid %llu\n", clt, lid);
    return ret;
}

lock_protocol::status lock_server::release(int clt, lock_protocol::lockid_t lid, int &r) {
    requests request;
    request.lockId = lid;
    request.client = clt;

    pthread_t thread;
    int res;
    void *status;

    res = pthread_create(&thread, NULL, &handleRelease, (void*) &request);
    if (res) {
        printf("ERROR: return code from pthread_create() is %d\n", res);
    }
    res = pthread_join(thread, &status);
    lock_protocol::status ret = (lock_protocol::status ) status;
    printf("release request from clt %d, lockid %llu\n", clt, lid);
    return ret;
}

