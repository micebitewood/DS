// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  ru = new release_user(ec);
  lock = new lock_client_cache(lock_dst, ru);
    srandom(getpid());
}

yfs_client::~yfs_client() {
    delete lock;
    delete ru;
    delete ec;
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  int r = OK;
  // You modify this function for Lab 3
  // - hold and release the file lock
  lock->acquire(inum);

  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu %lu %lu\n", inum, fin.size, fin.atime, fin.mtime);

 release:
 lock->release(inum);

  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;
  // You modify this function for Lab 3
  // - hold and release the directory lock
  lock->acquire(inum);

  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  std::cout << a.size << std::endl;
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
 lock->release(inum);
  return r;
}

//TODO done!
void yfs_client::parseDir(std::string buf, std::map<std::string, inum> &dir) {
    std::size_t start = 0;
    std::size_t pos = 0;
    std::size_t end = 0;
    
    std::cout << "parse dir: " << buf << std::endl;
    while ( (pos = buf.find('@', start)) != std::string::npos) {
        end = buf.find('#', pos + 1);
        std::string key = buf.substr(start, pos - start);
        inum value = n2i(buf.substr(pos + 1, end - pos - 1));
        dir[key] = value;
        start = end + 1;
    }
}

//TODO done!
int yfs_client::create(inum parent, std::string name, inum &ino, bool isFile) {
    int r = OK;
    lock->acquire(parent);
    std::cout << "lock aquired: " << parent << std::endl;
    ino = rand();
    if (isFile) 
        ino = (ino & 0xffffffff) | 0x80000000;
    else 
        ino = ino & 0x7fffffff;
    std::cout << "inum: " << ino << std::endl;
    std::ostringstream oss;
    std::string buf;
    int ret = get(parent, buf);
    if (ret != extent_protocol::OK) {
        r = NOENT;
        goto release;
    }
    if (buf.find(name + "@") == std::string::npos) {
        oss << buf;
        oss << name << '@' << ino << '#';
        std::string parentBuf = oss.str();

        lock->acquire(ino);
        if (ec->put(ino, "") != extent_protocol::OK) {
            r = RPCERR;
            goto release;
        }
        lock->release(ino);
        if (ec->put(parent, parentBuf) != extent_protocol::OK) {
            r = RPCERR;
            goto release;
        }
    } else {
        r = EXIST;
    }
release:
    lock->release(parent);
    std::cout << "lock released: " << parent << std::endl;
    return r;
}

int yfs_client::get(inum ino, std::string &buf) {
    int r = OK;
    if (ec->get(ino, buf) != extent_protocol::OK) {
        r = NOENT;
    }
    std::cout << "get in yfs client " << buf << std::endl;
    return r;
}

int yfs_client::read(inum ino, std::string &buf) {
    int r = OK;
    lock->acquire(ino);
    if (get(ino, buf) != extent_protocol::OK) {
        r = NOENT;
    }
    std::cout << "get in yfs client " << buf << std::endl;
    lock->release(ino);
    return r;
}

//TODO done!
int yfs_client::lookUp(inum parent, std::string name, inum &ino) {
    lock->acquire(parent);
    int r = OK;
    std::size_t pos = 0;
    std::size_t start = 0;
    std::size_t end = 0;
    std::string buf;
    if (get(parent, buf)!= extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    std::cout << "lookup: " << buf << std::endl;
    pos = buf.find(name + "@");
    if (pos == std::string::npos)
        r = NOENT;
    else {
        start = buf.find('@', pos + 1);
        end = buf.find('#', start + 1);
        ino = n2i(buf.substr(start + 1, end - start - 1));
    }
release:
    lock->release(parent);
    return r;
}

int yfs_client::unlink(inum parent, std::string name) {
    int r = OK;
    lock->acquire(parent);
    std::cout << "lock aquired: " << parent << std::endl;
    std::size_t pos = 0;
    std::size_t start = 0;
    std::size_t end = 0;
    std::ostringstream oss;
    std::string buf;
    std::string finalString;
    inum ino;
    std::cout << "name to be removed: " << name << std::endl;
    if (get(parent, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    std::cout << "unlink parent content: " << buf << std::endl;
    pos = buf.find(name + "@");
    if (pos == std::string::npos) {
        r = NOENT;
        goto release;
    }
    else {
        start = buf.find('@', pos + 1);
        end = buf.find('#', start + 1);
        ino = n2i(buf.substr(start + 1, end - start - 1));
    }
    oss << buf.substr(0, pos);
    oss << buf.substr(end + 1);
    finalString = oss.str();
    std::cout << "new parent content: " << finalString << ", size: " << finalString.size() << std::endl;

    lock->acquire(ino);
    if (ec->remove(ino) != extent_protocol::OK) {
        r = IOERR;
        lock->release(ino);
        goto release;
    }
    lock->release(ino);
    if (ec->put(parent, finalString) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    std::cout << "file is removed: " << ino << std::endl;

release:
    lock->release(parent);
    std::cout << "lock released: " << parent << std::endl;
    return r;
}

int yfs_client::setattr(inum ino, struct stat *attr) {
    lock->acquire(ino);
    std::cout << "lock aquired: " << ino << std::endl;
    int r = OK;
    std::string buf;
    std::string finalString;

    int ret = get(ino, buf);
    if (ret != OK) {
        r = NOENT;
        goto release;
    }
    if (buf.size() < attr->st_size) {
        std::ostringstream oss;
        oss << buf;
        for (int i = buf.size(); i < attr->st_size; i++) {
            oss << '\0';
        }
        finalString = oss.str();
    } else {
        finalString = buf.substr(0, attr->st_size);
    }

    if (ec->put(ino, finalString) != extent_protocol::OK) {
        r = RPCERR;
        goto release;
    }
release:
    lock->release(ino);
    std::cout << "lock released: " << ino << std::endl;
    return r;
}

int yfs_client::write(inum ino, std::string bufString, size_t size, off_t off) {
    lock->acquire(ino);
    int r = OK;
    std::cout << "lock aquired: " << ino << std::endl;
    std::cout << "size: " << bufString.size() << std::endl;
    std::string content;
    std::ostringstream oss;
    std::string finalString;
    int ret = get(ino, content);

    if (ret != OK) {
        r = NOENT;
        goto release;
    }

    if (off > content.size()) {
        oss << content;
        for (int i = content.size(); i < off; i++)
            oss << '\0';
        oss << bufString.substr(0, size);
    } else if (off + size > content.size()) {
        oss << content.substr(0, off);
        oss << bufString.substr(0, size);
    } else {
        oss << content.substr(0, off);
        oss << bufString.substr(0, size);
        oss << content.substr(off + size);
    }

    finalString = oss.str();
    if (ec->put(ino, finalString) != extent_protocol::OK) {
        r = RPCERR;
        goto release;
    }
release:
    lock->release(ino);
    std::cout << "lock released: " << ino << std::endl;
    return r;
}
