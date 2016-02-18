//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// BWTree.h
//
// Identification: src/backend/index/BWTree.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <functional>
#include <istream>
#include <ostream>
#include <memory>
#include <cstddef>
#include <assert.h>
#include <unordered_map>

#define BWTREE_MAX(a,b) ((a) < (b) ? (b) : (a))
#define BWTREE_NODE_SIZE 256
#define MAPPING_TABLE_SIZE 4194304

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyComparator>
class BWTree {

public:
  // *** Constructed Types

  typedef std::pair<KeyType, ValueType> PairType;

  typedef std::allocator<std::pair<KeyType, ValueType>> AllocType;

  typedef size_t PID;

public:
  // *** Static Constant Options and Values of the BW Tree

  static const PID NULL_PID = 0;

  static const unsigned short leaf_slot_max = BWTREE_MAX(8, BWTREE_NODE_SIZE / (sizeof(KeyType) + sizeof(ValueType)));

  static const unsigned short inner_slot_max = BWTREE_MAX(8, BWTREE_NODE_SIZE / (sizeof(KeyType) + sizeof(PID)));

  static const unsigned short min_leaf_slots = (leaf_slot_max / 2);

  static const unsigned short min_inner_slots = (inner_slot_max / 2);

private:
  // *** Node Classes for In-Memory Nodes

  /// The header structure of each node in memory. This structure is extended
  /// by inner_node or leaf_node or delta_node.
  struct node {
    unsigned short level;
    unsigned short slot_use;
    bool is_delta;

    inline void initialize(const unsigned short l, bool d) {
      level = l;
      slot_use = 0;
      is_delta = d;
    }

    inline bool isleafnode() const {
      return (level == 0);
    }

    inline bool isdelta() const {
      return is_delta;
    }
  };

  /// Extended structure of a inner node in memory. Contains only keys and no
  /// data items.
  struct inner_node : public node {

    typedef typename AllocType::template rebind<inner_node>::other alloc_type;

    KeyType slot_key[inner_slot_max];

    PID child_pid[inner_slot_max + 1];

    inline void initialize(const unsigned short l) {
      node::initialize(l, false);
    }

    inline bool isfull() const {
      return (node::slot_use == inner_slot_max);
    }

    inline bool isfew() const {
      return (node::slot_use <= min_inner_slots);
    }

    inline bool isunderflow() const {
      return (node::slot_use < min_inner_slots);
    }
  };

  /// Extended structure of a leaf node in memory. Contains pairs of keys and
  /// data items.
  struct leaf_node : public node {

    typedef typename AllocType::template rebind<leaf_node>::other alloc_type;

    PID prev_leaf;

    PID next_leaf;

    KeyType slot_key[leaf_slot_max];

    ValueType slot_data[leaf_slot_max];

    inline void initialize() {
      node::initialize(0, false);
      prev_leaf = next_leaf = NULL_PID;
    }

    inline bool isfull() const {
      return (node::slot_use == leaf_slot_max);
    }

    inline bool isfew() const {
      return (node::slot_use <= min_leaf_slots);
    }

    inline bool isunderflow() const {
      return (node::slot_use < min_leaf_slots);
    }

    inline void set_slot(unsigned short slot, const PairType &pair) {
      slot_key[slot] = pair.first;
      slot_data[slot] = pair.second;
    }

    inline void set_slot(unsigned short slot, const KeyType &key) {
      slot_key[slot] = key;
    }
  };

  /// Extended structure of a delta node in memory. Contains a physical
  /// pointer to base page.
  struct delta_node : public node {
    node *base;

    inline void initialize(const unsigned short l) {
      node::initialize(l, true);
      base = NULL;
    }

    inline void initialize(const unsigned short l, const node *n) {
      node::initialize(l, true);
      base = n;
    }

    inline void set_base(const node *n) {
      base = n;
    }
  };

  /// Extended structure of a delta node in memory. Contains a key, value
  /// pair to insert
  struct insert_node : public delta_node {
    typedef typename AllocType::template rebind<insert_node>::other alloc_type;

    KeyType insert_key;
    ValueType insert_value;

    inline void initialize(const unsigned short l, const PairType &pair) {
      delta_node::initialize(l);
      insert_key = pair.first;
      insert_value = pair.second;
    }
  };

  /// Extended structure of a delta node in memory. Contains a key to delete
  struct delete_node : public delta_node {
    typedef typename AllocType::template rebind<delete_node>::other alloc_type;

    KeyType delete_key;

    inline void initialize(const unsigned short l, const KeyType &key) {
      delta_node::initialize(l);
      delete_key = key;
    }
  };

  /// Extended structure of a delta node in memory. Contains a split key
  /// and a logical side pointer.
  struct split_node : public delta_node {
    KeyType split_key;
    PID side;

    inline void initialize(const unsigned short l, const KeyType &key, const PID pid) {
      delta_node::initialize(l);
      split_key = key;
      side = pid;
    }
  };

  /// Extended structure of a delta node in memory. Contains a key range
  /// [min_key, max_key) and a logical pointer to the leaf.
  struct separator_node : public delta_node {
    KeyType min_key;
    KeyType max_key;
    PID leaf;

    inline void initialize(const unsigned short l, const KeyType &left_key, const KeyType &right_key, const PID pid) {
      delta_node::initialize(l);
      min_key = left_key;
      max_key = right_key;
      leaf = pid;
    }
  };

  struct mapping_table {

    node** table;

    inline void initialize() {
      table = new node*[MAPPING_TABLE_SIZE];
    }

    // Atomically update the value using CAS
    inline void update(PID key, node* value) {
      for(;;) {
        if(__sync_bool_compare_and_swap(&table[key], table[key], value) == true) {
          break;  // Update success
        }
      }
    }

    // Mark as null if remove is called
    inline void remove(PID key) {
      for(;;) {
        if(__sync_bool_compare_and_swap(&table[key], table[key], NULL) == true) {
          break;  // Update success
        }
      }
    }

    // Get physical pointer from PID
    inline node* get(PID key) {
      return table[key];
    }

    ~mapping_table(){
      delete [] table;
    }
  };


private:
  // *** Tree Object Data Members

  /// Logical pointer to the BW tree's root node, either leaf or inner node
  PID m_root;

  /// Logical pointer to first leaf in the double linked leaf chain
  PID m_headleaf;

  /// Logical pointer to last leaf in the double linked leaf chain
  PID m_tailleaf;

  /// Memory allocator.
  AllocType m_allocator;

  /// Mapping table
  mapping_table mapping_table;

  /// Atomic counter for PID allocation
  std::atomic<int> pid_counter;

public:

  /// Default constructor initializing an empty BW tree with the standard key
  /// comparison function
  explicit inline BWTree(const AllocType &alloc = AllocType())
      : m_root(NULL_PID), m_headleaf(NULL_PID), m_tailleaf(NULL_PID), m_allocator(alloc) {
  }


  /// Frees up all used B+ tree memory pages
  inline ~BWTree() {
      // clear();
  }

private:
  // *** Convenient Key Comparison Functions Generated From key_less

  /// True if a < b ? "constructed" from m_key_less()
  inline bool key_less(const KeyType &a, const KeyType b) const {
    return KeyComparator(a, b);
  }

  /// True if a <= b ? constructed from key_less()
  inline bool key_lessequal(const KeyType &a, const KeyType b) const {
    return !KeyComparator(b, a);
  }

  /// True if a > b ? constructed from key_less()
  inline bool key_greater(const KeyType &a, const KeyType &b) const {
    return KeyComparator(b, a);
  }

  /// True if a >= b ? constructed from key_less()
  inline bool key_greaterequal(const KeyType &a, const KeyType b) const {
    return !KeyComparator(a, b);
  }

  /// True if a == b ? constructed from key_less(). This requires the <
  /// relation to be a total order, otherwise the B+ tree cannot be sorted.
  inline bool key_equal(const KeyType &a, const KeyType &b) const {
    return !KeyComparator(a, b) && !KeyComparator(b, a);
  }


private:
  // *** Node Object Allocation and Deallocation Functions

  typename leaf_node::alloc_type leaf_node_allocator() {
    return typename leaf_node::alloc_type(m_allocator);
  }

  /// Return an allocator for inner_node objects
  typename inner_node::alloc_type inner_node_allocator() {
    return typename inner_node::alloc_type(m_allocator);
  }

  /// Return an allocator for inner_node objects
  typename insert_node::alloc_type insert_node_allocator() {
    return typename insert_node::alloc_type(m_allocator);
  }

  /// Return an allocator for inner_node objects
  typename delete_node::alloc_type delete_node_allocator() {
    return typename delete_node::alloc_type(m_allocator);
  }

  /// Allocate and initialize a leaf node
  inline PID allocate_leaf() {
    leaf_node *n = new (leaf_node_allocator().allocate(1)) leaf_node();
    n->initialize();
    PID pid = allocate_pid();
    mapping_table.update(pid, n);
    return pid;
  }

  /// Allocate and initialize an inner node
  inline PID allocate_inner(unsigned short level) {
    inner_node *n = new (inner_node_allocator().allocate(1)) inner_node();
    n->initialize(level);
    PID pid = allocate_pid();
    mapping_table.update(pid, n);
    return pid;
  }

  /// Allocate and initialize an insert delta node
  inline insert_node *allocate_insert(unsigned short level, node *base) {
    insert_node *n = new (insert_node_allocator().allocate(1)) insert_node();
    n->initialize(level, base);
    return n;
  }

  /// Allocate and initialize an insert delta node
  inline delete_node *allocate_delete(unsigned short level, node *base) {
    insert_node *n = new (insert_node_allocator().allocate(1)) delete_node();
    n->initialize(level, base);
    return n;
  }

private:

  inline PID allocate_pid() {
    pid_counter++;
    return pid_counter;
  }

  inline node *get_node(PID pid) {
    if (mapping_table.get(pid) == NULL)
      return NULL;
    return mapping_table.get(pid);
  }

  inline void set_node(PID pid, node *n) {
    mapping_table.update(pid, n);
  }

private:
  template <typename node_type>
  inline unsigned short find_lower(const node_type *n, const KeyType &key) const{
    unsigned short lo = 0;
    while (lo < n->slotuse && key_less(n->slotkey[lo], key)) ++lo;
    return lo;
  }

public:
  inline void insert_data(const PairType &x);
  inline void delete_key(const KeyType &x);

};

}  // End index namespace
}  // End peloton namespace
