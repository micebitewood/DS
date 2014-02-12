#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>

#include "lock_protocol.h"
//#include "lock_client.h"
#include "lock_client_cache.h"

class yfs_client {
  extent_client *ec;
    //lock_client *lock;
    lock_client_cache *lock;
    lock_release_user *ru;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    yfs_client::inum inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);
 public:

  yfs_client(std::string, std::string);
  ~yfs_client();

  bool isfile(inum);
  bool isdir(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);
  void parseDir(std::string, std::map<std::string, inum> &);
  int put(inum, std::string);
  int create(inum, std::string, inum &, bool);
  int get(inum, std::string &);
  int read(inum, std::string &);
  int lookUp(inum, std::string, inum &);
  int unlink(inum, std::string);
  int setattr(inum, struct stat *);
  int write(inum, std::string, size_t, off_t);
};

#endif 
