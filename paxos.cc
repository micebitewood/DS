#include "paxos.h"
#include "handle.h"
// #include <signal.h>
#include <stdio.h>
#include "tprintf.h"
#include "lang/verify.h"

// This module implements the proposer and acceptor of the Paxos
// distributed algorithm as described by Lamport's "Paxos Made
// Simple".  To kick off an instance of Paxos, the caller supplies a
// list of nodes, a proposed value, and invokes the proposer.  If the
// majority of the nodes agree on the proposed value after running
// this instance of Paxos, the acceptor invokes the upcall
// paxos_commit to inform higher layers of the agreed value for this
// instance.


bool
operator> (const prop_t &a, const prop_t &b)
{
  return (a.n > b.n || (a.n == b.n && a.m > b.m));
}

bool
operator>= (const prop_t &a, const prop_t &b)
{
  return (a.n > b.n || (a.n == b.n && a.m >= b.m));
}

std::string
print_members(const std::vector<std::string> &nodes)
{
  std::string s;
  s.clear();
  for (unsigned i = 0; i < nodes.size(); i++) {
    s += nodes[i];
    if (i < (nodes.size()-1))
      s += ",";
  }
  return s;
}

bool isamember(std::string m, const std::vector<std::string> &nodes)
{
  for (unsigned i = 0; i < nodes.size(); i++) {
    if (nodes[i] == m) return 1;
  }
  return 0;
}

bool
proposer::isrunning()
{
  bool r;
  ScopedLock ml(&pxs_mutex);
  r = !stable;
  return r;
}

// check if the servers in l2 contains a majority of servers in l1
bool
proposer::majority(const std::vector<std::string> &l1, 
		const std::vector<std::string> &l2)
{
  unsigned n = 0;

  for (unsigned i = 0; i < l1.size(); i++) {
    if (isamember(l1[i], l2))
      n++;
  }
  return n >= (l1.size() >> 1) + 1;
}

proposer::proposer(class paxos_change *_cfg, class acceptor *_acceptor, 
		   std::string _me)
  : cfg(_cfg), acc (_acceptor), me (_me), break1 (false), break2 (false), 
    stable (true)
{
  VERIFY (pthread_mutex_init(&pxs_mutex, NULL) == 0);
  my_n.n = 0;
  my_n.m = me;
}

void
proposer::setn()
{
  my_n.n = acc->get_n_h().n + 1 > my_n.n + 1 ? acc->get_n_h().n + 1 : my_n.n + 1;
}

bool
proposer::run(int instance, std::vector<std::string> cur_nodes, std::string newv)
{
  std::vector<std::string> accepts;
  std::vector<std::string> nodes;
  std::string v;
  bool r = false;

  ScopedLock ml(&pxs_mutex);
  tprintf("start: initiate paxos for %s w. i=%d v=%s stable=%d\n",
	 print_members(cur_nodes).c_str(), instance, newv.c_str(), stable);
  if (!stable) {  // already running proposer?
    tprintf("proposer::run: already running\n");
    return false;
  }
  stable = false;
  setn();
  accepts.clear();
  v.clear();
  if (prepare(instance, accepts, cur_nodes, v)) {

    if (majority(cur_nodes, accepts)) {
      tprintf("paxos::manager: received a majority of prepare responses\n");

      if (v.size() == 0)
	v = newv;

      breakpoint1();

      nodes = accepts;
      accepts.clear();
      accept(instance, accepts, nodes, v);

      if (majority(cur_nodes, accepts)) {
	tprintf("paxos::manager: received a majority of accept responses\n");

	breakpoint2();

	decide(instance, accepts, v);
	r = true;
      } else {
	tprintf("paxos::manager: no majority of accept responses\n");
      }
    } else {
      tprintf("paxos::manager: no majority of prepare responses\n");
    }
  } else {
    tprintf("paxos::manager: prepare is rejected %d\n", stable);
  }
  stable = true;
  return r;
}

// proposer::run() calls prepare to send prepare RPCs to nodes
// and collect responses. if one of those nodes
// replies with an oldinstance, return false.
// otherwise fill in accepts with set of nodes that accepted,
// set v to the v_a with the highest n_a, and return true.
bool
proposer::prepare(unsigned instance, std::vector<std::string> &accepts, 
         std::vector<std::string> nodes,
         std::string &v)
{
    std::vector<std::string>::iterator it;
    std::cout << "prepare: " << me << std::endl;
    std::cout << "current instance: " << instance << std::endl;
    my_n.n += rand() % 100 + 1;
    std::cout << "my_n: " << my_n.n << " " << my_n.m << std::endl;
    prop_t temp;
    temp.n = 0;
    for (it = nodes.begin(); it < nodes.end(); it++) {
        std::cout << " nodes: " << *it << std::endl;
        handle h(*it);
        rpcc *cl = h.safebind();
        //n.n += rand() % 100 + 1;
restart:
        paxos_protocol::preparearg a;
        a.instance = instance;
        a.n = my_n;
        paxos_protocol::prepareres r;
        int ret = rpc_const::timeout_failure;
        if (cl)
          ret = cl->call(paxos_protocol::preparereq, me, a, r, rpcc::to(1000));
        std::cout << "prepare:  call done" << std::endl;
        if (ret == paxos_protocol::OK) {
          if (r.oldinstance) {
            std::cout << "prepare:  oldinstance " << me << std::endl;
            acc->commit(r.n_a.n, r.v_a);
            return false;
          } else if (r.accept) {
            std::cout << "prepare:  accepted by " << *it << std::endl;
            accepts.push_back(*it);
            if (r.n_a > temp) {
              temp = r.n_a;
              v = r.v_a;
              std::cout << "prepare: new my_n & v: " << my_n.n << " " << my_n.m << ", " << v << std::endl;
            }
          } else {
            std::cout << "prepare:  rejected by " << *it << std::endl;
            std::cout << "  r.n_a: " << r.n_a.n << " " << r.n_a.m << std::endl;
            std::cout << "  r.v_a: " << r.v_a << std::endl;
          }
        } else if (ret == rpc_const::timeout_failure) {
          std::cout << "prepare:  timeout" << std::endl;
        } else {
          std::cout << "prepare:  wtf" << std::endl;
          goto restart;
        }
    }
    std::cout << "prepare:  end" << std::endl;
  // You fill this in for Lab 6
  // Note: if got an "oldinstance" reply, commit the instance using
  // acc->commit(...), and return false.
  return true;
}

// run() calls this to send out accept RPCs to accepts.
// fill in accepts with list of nodes that accepted.
void
proposer::accept(unsigned instance, std::vector<std::string> &accepts,
        std::vector<std::string> nodes, std::string v)
{
    std::cout << "accept: " << me << std::endl;
    std::vector<std::string>::iterator it;
    for (it = nodes.begin(); it < nodes.end(); it++) {
      std::cout << " nodes: " << *it << std::endl;
      handle h(*it);
      paxos_protocol::acceptarg a;
      a.instance = instance;
      a.n = my_n;
      a.v = v;
      bool r;
      rpcc *cl = h.safebind();
      cl->call(paxos_protocol::acceptreq, me, a, r, rpcc::to(1000));
      if (r) {
        accepts.push_back(*it);
      }
    }
  // You fill this in for Lab 6
}

void
proposer::decide(unsigned instance, std::vector<std::string> accepts, 
	      std::string v)
{
    std::cout << "decide: " << me << std::endl;
    std::vector<std::string>::iterator it;
    for (it = accepts.begin(); it < accepts.end(); it++) {
      std::cout << " nodes: " << *it << std::endl;
      handle h(*it);
      paxos_protocol::decidearg a;
      a.instance = instance;
      a.v = v;
      rpcc *cl = h.safebind();
      int r;
      cl->call(paxos_protocol::decidereq, me, a, r, rpcc::to(1000));
    }
  // You fill this in for Lab 6
}

acceptor::acceptor(class paxos_change *_cfg, bool _first, std::string _me, 
	     std::string _value)
  : cfg(_cfg), me (_me), instance_h(0)
{
  VERIFY (pthread_mutex_init(&pxs_mutex, NULL) == 0);

  n_h.n = 0;
  n_h.m = me;
  n_a.n = 0;
  n_a.m = me;
  v_a.clear();

  l = new log (this, me);

  if (instance_h == 0 && _first) {
    values[1] = _value;
    l->loginstance(1, _value);
    instance_h = 1;
  }

  pxs = new rpcs(atoi(_me.c_str()));
  pxs->reg(paxos_protocol::preparereq, this, &acceptor::preparereq);
  pxs->reg(paxos_protocol::acceptreq, this, &acceptor::acceptreq);
  pxs->reg(paxos_protocol::decidereq, this, &acceptor::decidereq);
}

paxos_protocol::status
acceptor::preparereq(std::string src, paxos_protocol::preparearg a,
    paxos_protocol::prepareres &r)
{
  std::cout << "  preparereq: " << src << std::endl;
  std::cout << "  req.instance: " << a.instance << std::endl;
  std::cout << "  instance_h: " << instance_h << std::endl;
  std::cout << "  value of instance_h: " << values[instance_h] << std::endl;
  std::cout << "  req.n: " << a.n.n << " " << a.n.m << std::endl;
  std::cout << "  n_h: " << n_h.n << " " << n_h.m << std::endl;
  std::cout << "  n_a: " << n_a.n << " " << n_a.m << std::endl;
  std::cout << "  v_a: " << v_a << std::endl;
  if (a.instance <= instance_h) {
    prop_t t;
    t.n = instance_h;

    r.oldinstance = true;
    r.accept = false;
    r.n_a = t;
    r.v_a = values[instance_h];
    return paxos_protocol::OK;
  } else if (a.n > n_h) {
    n_h = a.n;
    r.oldinstance = false;
    r.accept = true;
    r.n_a = n_a;
    r.v_a = v_a;
    l->logprop(n_h);
    return paxos_protocol::OK;
  } 

  r.oldinstance = false;
  r.accept = false;
  r.n_a = n_h;
  r.v_a = v_a;
  return paxos_protocol::OK;
  // You fill this in for Lab 6
  // Remember to initialize *BOTH* r.accept and r.oldinstance appropriately.
  // Remember to *log* the proposal if the proposal is accepted.
  // return paxos_protocol::OK;

}

// the src argument is only for debug purpose
paxos_protocol::status
acceptor::acceptreq(std::string src, paxos_protocol::acceptarg a, bool &r)
{
  // You fill this in for Lab 6
  // Remember to *log* the accept if the proposal is accepted.

  std::cout << "   acceptreq: " << src << std::endl;
  std::cout << "   req.instance: " << a.instance << std::endl;
  std::cout << "   instance_h: " << instance_h << std::endl;
  std::cout << "   req.n: " << a.n.n << " " << a.n.m << std::endl;
  std::cout << "   n_h: " << n_h.n << " " << n_h.m << std::endl;
  std::cout << "   req.v: " << a.v << std::endl;
  std::cout << "   v_a: " << v_a << std::endl;

  if (a.n >= n_h) {
    n_a = a.n;
    v_a = a.v;
    std::cout << "   v_a changed to: " << a.v << std::endl;
    r = true;
    l->logaccept(n_a, v_a);
  } else {
    r = false;
  }
  return paxos_protocol::OK;
}

// the src argument is only for debug purpose
paxos_protocol::status
acceptor::decidereq(std::string src, paxos_protocol::decidearg a, int &r)
{
  ScopedLock ml(&pxs_mutex);
  std::cout << "    decidereq: " << src << std::endl;
  std::cout << "    v: " << a.v << std::endl;
  tprintf("decidereq for accepted instance %d (my instance %d) v=%s\n", 
	 a.instance, instance_h, v_a.c_str());
  if (a.instance == instance_h + 1) {
    VERIFY(v_a == a.v);
    commit_wo(a.instance, v_a);
  } else if (a.instance <= instance_h) {
    // we are ahead ignore.
  } else {
    // we are behind
    VERIFY(0);
  }
  return paxos_protocol::OK;
}

void
acceptor::commit_wo(unsigned instance, std::string value)
{
  //assume pxs_mutex is held
  tprintf("acceptor::commit: instance=%d has v= %s\n", instance, value.c_str());
  if (instance > instance_h) {
    tprintf("commit: highestaccepteinstance = %d\n", instance);
    values[instance] = value;
    l->loginstance(instance, value);
    instance_h = instance;
    n_h.n = 0;
    n_h.m = me;
    n_a.n = 0;
    n_a.m = me;
    v_a.clear();
    if (cfg) {
      pthread_mutex_unlock(&pxs_mutex);
      cfg->paxos_commit(instance, value);
      pthread_mutex_lock(&pxs_mutex);
    }
  }
}

void
acceptor::commit(unsigned instance, std::string value)
{
  ScopedLock ml(&pxs_mutex);
  commit_wo(instance, value);
}

std::string
acceptor::dump()
{
  return l->dump();
}

void
acceptor::restore(std::string s)
{
  l->restore(s);
  l->logread();
}



// For testing purposes

// Call this from your code between phases prepare and accept of proposer
void
proposer::breakpoint1()
{
  if (break1) {
    tprintf("Dying at breakpoint 1!\n");
    exit(1);
  }
}

// Call this from your code between phases accept and decide of proposer
void
proposer::breakpoint2()
{
  if (break2) {
    tprintf("Dying at breakpoint 2!\n");
    exit(1);
  }
}

void
proposer::breakpoint(int b)
{
  if (b == 3) {
    tprintf("Proposer: breakpoint 1\n");
    break1 = true;
  } else if (b == 4) {
    tprintf("Proposer: breakpoint 2\n");
    break2 = true;
  }
}
