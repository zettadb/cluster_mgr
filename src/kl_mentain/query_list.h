/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _CLUSTER_MGR_QUERY_LIST_H_
#define _CLUSTER_MGR_QUERY_LIST_H_
#include <string>
#include <cassert>
#include "util_func/object_ptr.h"

namespace kunlun
{

/*
* double-list c type
*/
#define UT_LIST_NODE_T(TYPE)                                      \
  struct {                                                        \
    TYPE *prev; /*!< pointer to the previous node,                \
                NULL if start of list */                          \
    TYPE *next; /*!< pointer to next node, NULL if end of list */ \
  }

#define UT_LIST_BASE_NODE_T(TYPE)                             \
  struct {                                                    \
    int count;   /*!< count of nodes in list */               \
    TYPE *start; /*!< pointer to list start, NULL if empty */ \
    TYPE *end;   /*!< pointer to list end, NULL if empty */   \
  }

/** Some Macros to manipulate the list, extracted from "ut0lst.h" */
#define UT_LIST_INIT(BASE) \
  {                        \
    (BASE).count = 0;      \
    (BASE).start = NULL;   \
    (BASE).end = NULL;     \
  }

#define UT_LIST_ADD_LAST(NAME, BASE, N) \
  {                                     \
    ((BASE).count)++;                   \
    ((N)->NAME).prev = (BASE).end;      \
    ((N)->NAME).next = NULL;            \
    if ((BASE).end != NULL) {           \
      (((BASE).end)->NAME).next = (N);  \
    }                                   \
    (BASE).end = (N);                   \
    if ((BASE).start == NULL) {         \
      (BASE).start = (N);               \
    }                                   \
  }

#define UT_LIST_ADD_FIRST(NAME, BASE, N)     \
  {                                          \
    ((BASE).count)++;                        \
    ((N)->NAME).next = (BASE).start;         \
    ((N)->NAME).prev = NULL;                 \
    if (KL_LIKELY((BASE).start != NULL)) { \
      (((BASE).start)->NAME).prev = (N);     \
    }                                        \
    (BASE).start = (N);                      \
    if (KL_UNLIKELY((BASE).end == NULL)) { \
      (BASE).end = (N);                      \
    }                                        \
  }

#define UT_LIST_REMOVE_CLEAR(NAME, N) \
  ((N)->NAME.prev = (N)->NAME.next =  \
       reinterpret_cast<decltype((N)->NAME.next)>(-1))

/** Removes a node from a linked list. */
#define UT_LIST_REMOVE(NAME, BASE, N)                     \
  do {                                                    \
    ((BASE).count)--;                                     \
    if (((N)->NAME).next != NULL) {                       \
      ((((N)->NAME).next)->NAME).prev = ((N)->NAME).prev; \
    } else {                                              \
      (BASE).end = ((N)->NAME).prev;                      \
    }                                                     \
    if (((N)->NAME).prev != NULL) {                       \
      ((((N)->NAME).prev)->NAME).next = ((N)->NAME).next; \
    } else {                                              \
      (BASE).start = ((N)->NAME).next;                    \
    }                                                     \
    UT_LIST_REMOVE_CLEAR(NAME, N);                        \
  } while (0)

#define UT_LIST_GET_NEXT(NAME, N) (((N)->NAME).next)

#define UT_LIST_GET_LEN(BASE) (BASE).count

#define UT_LIST_GET_FIRST(BASE) (BASE).start

/*
* lock base on atomic_flag
*/
class AtomicLock {
public:
    AtomicLock() {
    }
    virtual ~AtomicLock() {}

    void lock() { while(flag_.test_and_set(std::memory_order_acquire)); }
    void unlock() { flag_.clear(std::memory_order_release); }
private:
    std::atomic_flag flag_ = ATOMIC_FLAG_INIT;
};

}
#endif
