// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <algorithm>

// The calls assume that the caller holds a lock on the extent

int getCurrTime() {
    time_t timer;
    time(&timer);
    return timer;
}

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
  pthread_mutex_init(&mutex, NULL);
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&mutex);
  if (std::find(deleteBuf.begin(), deleteBuf.end(), eid) != deleteBuf.end()) {
    ret = extent_protocol::NOENT;
  } else if (bufMap.find(eid) == bufMap.end()) { 
    std::cout << "  get call" << std::endl;
    pthread_mutex_unlock(&mutex);
    ret = cl->call(extent_protocol::get, eid, buf);
    pthread_mutex_lock(&mutex);
    if (ret == extent_protocol::OK) {
        std::cout << "  call done: " << buf << std::endl;
        bufMap[eid] = buf;
        extent_protocol::attr attr;
        pthread_mutex_unlock(&mutex);
        ret = cl->call(extent_protocol::getattr, eid, attr);
        pthread_mutex_lock(&mutex);
        if (ret == extent_protocol::OK) {
            attr.atime = getCurrTime();
            attrMap[eid] = attr;
        }
    }
  } else {
    buf = bufMap[eid];
    std::cout << "  get local: " << buf << std::endl;
    attrMap[eid].atime = getCurrTime();
    ret = extent_protocol::OK;
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&mutex);
  if (std::find(deleteBuf.begin(), deleteBuf.end(), eid) != deleteBuf.end()) {
    ret = extent_protocol::NOENT;
  } else if (attrMap.find(eid) == attrMap.end()) {
    pthread_mutex_unlock(&mutex);
    ret = cl->call(extent_protocol::getattr, eid, attr);
    pthread_mutex_lock(&mutex);
    if (ret == extent_protocol::OK) {
        attrMap[eid] = attr;
    }
  } else {
    attr = attrMap[eid];
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&mutex);
  std::cout << eid << " put: " << buf << std::endl;
  int currTime = getCurrTime();
  std::vector<extent_protocol::extentid_t>::iterator it;
  if ((it = std::find(deleteBuf.begin(), deleteBuf.end(), eid)) != deleteBuf.end()) {
    deleteBuf.erase(it);
  }
  if (bufMap.find(eid) == bufMap.end()) {
    extent_protocol::attr attr;
    attr.size = buf.size();
    attr.atime = 0;
    attr.mtime = currTime;
    attr.ctime = currTime;
    attrMap[eid] = attr;
  } else {
    attrMap[eid].size = buf.size();
    attrMap[eid].mtime = currTime;
    attrMap[eid].ctime = currTime;
  }
  bufMap[eid] = buf;
  if (std::find(dirtyBuf.begin(), dirtyBuf.end(), eid) == dirtyBuf.end())
    dirtyBuf.push_back(eid);
  pthread_mutex_unlock(&mutex);
  //ret = cl->call(extent_protocol::put, eid, buf, r);
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&mutex);
  if (bufMap.find(eid) != bufMap.end()) {
    if (std::find(deleteBuf.begin(), deleteBuf.end(), eid) == deleteBuf.end()) {
        deleteBuf.push_back(eid);
        if (std::find(dirtyBuf.begin(), dirtyBuf.end(), eid) == dirtyBuf.end())
            dirtyBuf.push_back(eid);
    }
    else
        ret = extent_protocol::NOENT;
  }
  pthread_mutex_unlock(&mutex);
  //ret = cl->call(extent_protocol::remove, eid, r);
  return ret;
}

void extent_client::flush(extent_protocol::extentid_t eid) {
    int r;
    std::vector<extent_protocol::extentid_t>::iterator vecIt;
    std::map<extent_protocol::extentid_t, std::string>::iterator bufIt;
    pthread_mutex_lock(&mutex);
    std::cout << eid << " get mutex lock" << std::endl;
    if ((vecIt = std::find(deleteBuf.begin(), deleteBuf.end(), eid)) != deleteBuf.end()) {
        std::cout << eid << " deleted" << std::endl;
        cl->call(extent_protocol::remove, eid, r);
        dirtyBuf.erase(std::find(dirtyBuf.begin(), dirtyBuf.end(), eid));
        if ((bufIt = bufMap.find(eid)) != bufMap.end()) {
            bufMap.erase(bufIt);
            attrMap.erase(attrMap.find(eid));
        }
    } else if ((vecIt = std::find(dirtyBuf.begin(), dirtyBuf.end(), eid)) != dirtyBuf.end()) {
        std::cout << eid << " flushed: " << bufMap[eid] << std::endl;
        cl->call(extent_protocol::put, eid, bufMap[eid], r);
        std::string temp;
        cl->call(extent_protocol::get, eid, temp);
        std::cout << eid << " check: " << temp << std::endl;
        bufMap.erase(bufMap.find(eid));
        attrMap.erase(attrMap.find(eid));
        dirtyBuf.erase(vecIt);
    } else {
        if (bufMap.find(eid) != bufMap.end())
            bufMap.erase(bufMap.find(eid));
        if (attrMap.find(eid) != attrMap.end())
            attrMap.erase(attrMap.find(eid));
    }
    /*
    for (vecIt = deleteBuf.begin(); vecIt < deleteBuf.end(); vecIt++) {
        std::cout << eid << " deleted" << std::endl;
        cl->call(extent_protocol::remove, eid, r);
        dirtyBuf.erase(std::find(dirtyBuf.begin(), dirtyBuf.end(), eid));
        if ((bufIt = bufMap.find(eid)) != bufMap.end()) {
            bufMap.erase(bufIt);
            attrMap.erase(attrMap.find(eid));
        }
    }
    for ( vecIt = dirtyBuf.begin(); vecIt < dirtyBuf.end(); vecIt++) {
        std::cout << eid << " flushed: " << bufMap[eid] << std::endl;
        cl->call(extent_protocol::put, eid, bufMap[eid], r);
        bufMap.erase(bufMap.find(eid));
        attrMap.erase(attrMap.find(eid));
        dirtyBuf.erase(vecIt);
    }
    */
    pthread_mutex_unlock(&mutex);
}

