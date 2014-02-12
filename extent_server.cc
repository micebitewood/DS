// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>

int getCurrTime() {
    time_t timer;
    time(&timer);
    std::cout << "getCurrTime : " << timer << std::endl;
    return timer;
}

extent_server::extent_server() {
    contents[1] = "";
    extent_protocol::attr attr;
    int currTime = getCurrTime();
    attr.size = 0;
    attr.atime = 0;
    attr.mtime = currTime;
    attr.ctime = currTime;
    attributes[1] = attr;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  int currTime = getCurrTime();
  std::cout << " new put: " << id << "," << buf << std::endl; 
  if (contents.find(id) == contents.end()) {
    extent_protocol::attr attr;
    attr.size = buf.size();
    attr.atime = 0;
    attr.mtime = currTime;
    attr.ctime = currTime;
    contents[id] = buf;
    attributes[id] = attr;
  }
  else {
    contents[id] = buf;
    attributes[id].size = buf.size();
    attributes[id].mtime = currTime;
    attributes[id].ctime = currTime;
  }
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  std::cout << "  new get: " << id << std::endl; 
  if (contents.find(id) != contents.end()) {
    buf = contents[id];
    std::cout << "  get done: " << buf << std::endl;
    attributes[id].atime = getCurrTime();
    return extent_protocol::OK;
  }
  return extent_protocol::NOENT;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  // You replace this with a real implementation. We send a phony response
  // for now because it's difficult to get FUSE to do anything (including
  // unmount) if getattr fails.
  if (attributes.find(id) != attributes.end()) {
    a = attributes[id];
    return extent_protocol::OK;
  }
  return extent_protocol::NOENT;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  if (contents.find(id) != contents.end()) {
    contents.erase(id);
    std::cout << "serverside, remove content: " << id << std::endl;
    attributes.erase(id);
    std::cout << "serverside, remove attributes: " << id << std::endl;
    return extent_protocol::OK;
  }
  return extent_protocol::NOENT;
}

