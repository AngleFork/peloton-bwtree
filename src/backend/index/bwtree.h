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
#include <unordered_set>
#include <atomic>

#define BWTREE_MAX(a,b) ((a) < (b) ? (b) : (a))
#define BWTREE_NODE_SIZE 256
#define MAPPING_TABLE_SIZE 4194304

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
class BWTree {

public:
  // *** Constructed Types

  typedef size_t PID;

  typedef std::pair<KeyType, ValueType> DataPairType;
  typedef std::pair<KeyType, ValueType> PointerPairType;

  typedef std::allocator<std::pair<KeyType, ValueType>> AllocType;


  enum class NodeType {
    leaf_node,
    inner_node,
    insert_node,
    delete_node,
    split_node,
    separator_node
  };

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
    NodeType node_type;
    unsigned short level;
    unsigned short slot_use;

    PID parent;

    inline void initialize(NodeType n, unsigned short l, unsigned short s) {
      node_type = n;
      level = l;
      slot_use = s;
      parent = NULL_PID;
    }

    inline bool is_leaf() const {
      return (level == 0);
    }

    inline bool is_delta() const {
      return (node_type != NodeType::leaf_node && node_type != NodeType::inner_node);
    }

    inline bool is_full() const {
      return (slot_use == leaf_slot_max);
    }

    inline bool is_few() const {
      return (slot_use <= min_leaf_slots);
    }

    inline bool is_underflow() const {
      return (slot_use < min_leaf_slots);
    }

    inline NodeType get_type() const {
      return node_type;
    }

    inline unsigned short get_level() const {
      return level;
    }

    inline size_t get_size() const {
      return slot_use;
    }

    inline void add_slotuse() {
      slot_use++;
    }

    inline PID get_parent() const {
      return parent;
    }

    inline void set_parent(PID p) {
      parent = p;
    }

  };

  /// Extended structure of a inner node in memory. Contains only keys and no
  /// data items.
  struct inner_node : public node {

    typedef typename AllocType::template rebind<inner_node>::other alloc_type;

    KeyType slot_key[inner_slot_max];

    PID child_pid[inner_slot_max + 1];

    inline void initialize(unsigned short l) {
      node::initialize(NodeType::inner_node, l, 0);
    }

    inline void set_slot(unsigned short slot, KeyType k, PID p) {
      if (slot >= node::get_size())
          node::add_slotuse();
      slot_key[slot] = k;
      child_pid[slot] = p;
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
      node::initialize(NodeType::leaf_node, 0, 0);
      prev_leaf = next_leaf = NULL_PID;
    }

    inline void set_slot(unsigned short slot, const DataPairType &pair) {
      if (slot >= node::get_size())
          node::add_slotuse();
      slot_key[slot] = pair.first;
      slot_data[slot] = pair.second;
    }

    inline PID get_prev() const {
      return prev_leaf;
    }

    inline void set_prev(PID pid) {
      prev_leaf = pid;
    }

    inline PID get_next() const {
      return next_leaf;
    }

    inline void set_next(PID pid) {
      next_leaf = pid;
    }

  };

  /// Extended structure of a delta node in memory. Contains a physical
  /// pointer to base page.
  struct delta_node : public node {
    node *base;
    size_t chain_length;

    inline void initialize(NodeType t, unsigned short s, node *n) {
      base = n;
      chain_length = 0;
      if (base->is_delta()) {
        chain_length = static_cast<delta_node *>(base)->get_length() + 1;
      }
      node::initialize(t, base->get_level(), s);
    }

    inline void set_base(node *n) {
      base = n;
    }

    inline node *get_base() const {
      return base;
    }

    inline size_t get_length() const {
      return chain_length;
    }
  };

  /// Extended structure of a delta node in memory. Contains a key, value
  /// pair to insert
  struct insert_node : public delta_node {
    typedef typename AllocType::template rebind<insert_node>::other alloc_type;

    KeyType insert_key;
    ValueType insert_value;

    inline void initialize(const DataPairType &pair, node *n) {
      insert_key = pair.first;
      insert_value = pair.second;
      delta_node::initialize(NodeType::insert_node, n->get_size() + 1, n);
    }

    inline DataPairType get_data() const {
      return std::make_pair(insert_key, insert_value);
    }
  };

  /// Extended structure of a delta node in memory. Contains a key to delete
  struct delete_node : public delta_node {
    typedef typename AllocType::template rebind<delete_node>::other alloc_type;

    KeyType delete_key;

    inline void initialize(const KeyType &key, node *n) {
      delete_key = key;
      delta_node::initialize(NodeType::delete_node, n->get_size() - 1, n);
    }

    inline KeyType get_key() const {
      return delete_key;
    }
  };

  /// Extended structure of a delta node in memory. Contains a split key
  /// and a logical side pointer.
  struct split_node : public delta_node {
    typedef typename AllocType::template rebind<split_node>::other alloc_type;

    KeyType split_key;
    PID side;

    inline void initialize(const KeyType &key, PID pid, unsigned short s, node *n) {
      split_key = key;
      side = pid;
      delta_node::initialize(NodeType::split_node, s, n);
    }

    inline KeyType get_key() const {
      return split_key;
    }
  };

  /// Extended structure of a delta node in memory. Contains a key range
  /// [min_key, max_key) and a logical pointer to the leaf.
  struct separator_node : public delta_node {
    typedef typename AllocType::template rebind<separator_node>::other alloc_type;

    KeyType min_key;
    KeyType max_key;
    PID leaf;

    inline void initialize(const KeyType &left_key, const KeyType &right_key, const PID pid, node *n) {
      min_key = left_key;
      max_key = right_key;
      leaf = pid;
      delta_node::initialize(NodeType::separator_node, n->get_size() + 1, n);
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

  /// True if a < b ? "constructed" from m_key_less()
  inline bool datapair_less(const DataPairType &a, const DataPairType b) const {
    return KeyComparator(a.first, b.first);
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

  /// Return an allocator for insert_node objects
  typename insert_node::alloc_type insert_node_allocator() {
    return typename insert_node::alloc_type(m_allocator);
  }

  /// Return an allocator for delete_node objects
  typename delete_node::alloc_type delete_node_allocator() {
    return typename delete_node::alloc_type(m_allocator);
  }

  /// Return an allocator for split_node objects
  typename split_node::alloc_type split_node_allocator() {
    return typename split_node::alloc_type(m_allocator);
  }

  /// Return an allocator for separator_node objects
  typename separator_node::alloc_type separator_node_allocator() {
    return typename separator_node::alloc_type(m_allocator);
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
  inline insert_node *allocate_insert(DataPairType &pair, node *base) {
    insert_node *n = new (insert_node_allocator().allocate(1)) insert_node();
    n->initialize(pair, base);
    return n;
  }

  /// Allocate and initialize an delete delta node
  inline delete_node *allocate_delete(KeyType &key, node *base) {
    delete_node *n = new (delete_node_allocator().allocate(1)) delete_node();
    n->initialize(key, base);
    return n;
  }

  /// Allocate and initialize an split delta node
  inline split_node *allocate_split(KeyType &key, PID leaf, unsigned short size, node *base) {
    split_node *n = new (insert_node_allocator().allocate(1)) insert_node();
    n->initialize(key, leaf, size, base);
    return n;
  }

  /// Allocate and initialize an separator delta node
  inline separator_node *allocate_separator(KeyType &left_key, KeyType &right_key, PID leaf, node *base) {
    split_node *n = new (insert_node_allocator().allocate(1)) insert_node();
    n->initialize(left_key, right_key, leaf, base);
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
  inline unsigned short find_lower(const node_type *n, const KeyType &key) const {
    unsigned short lo = 0;
    while (lo < n->slot_use && key_less(n->slot_key[lo], key)) ++lo;
    return lo;
  }

  inline node *get_base_node(node *n) const {
    while (n->is_delta()) {
      n = static_cast<delta_node *>(n)->get_base();
    }
    return n;
  }

  inline std::vector<DataPairType> get_all_data(node *n) const {
    std::unordered_set<DataPairType> inserted;
    std::unordered_set<KeyType> deleted;
    bool has_split = false;
    KeyType split_key;

    while (n->is_delta()) {
      switch (n->get_type()) {
        case NodeType::insert_node:
          DataPairType data = static_cast<insert_node *>(n)->get_data();
          if ((!has_split || key_less(data.first, split_key))
              && deleted.find(data) == deleted.end()) {
            inserted.insert(data);
          }
          break;

        case NodeType::delete_node:
          KeyType key = static_cast<delete_node *>(n)->get_key();
          deleted.insert(key);
          break;

        case NodeType::split_node:
          if (!has_split) {
            split_key = static_cast<split_node *>(n)->get_key();
            has_split = true;
          }
          break;
      }
      n = static_cast<delta_node *>(n)->get_base();
    }

    std::vector<DataPairType> result;
    for (const auto &data: inserted) {
      result.push_back(data);
    }
    for (unsigned short slot = 0; slot < n->get_size(); slot++) {
      result.push_back(std::make_pair(n->slot_key[slot], n->slot_data[slot]));
    }
    std::sort(result.begin(), result.end(), datapair_less);
    return result;
  }

  // Helper function for checking if the key is in the vector.
  inline bool vector_contains_key(std::vector<DataPairType> data, const KeyType &key) {
    for (auto it = data.begin() ; it != data.end(); ++it) {
      if(key_equal(key, it->first)) {
        return true;
      }
    }
    return false;
  }

  // Returns the pid of the page that contains the target key
  // Currently, returns -1 for error
  inline PID get_leaf_node_pid(const KeyType &key) {
    PID current_pid = m_root;
    node* current = mapping_table.get(m_root);

    if(!current) return -1;

    // Keep traversing tree until we find the target leaf node
    while(!current->is_leaf()) {
      NodeType current_type = current->get_type();
      // We need to take care of delta nodes from split/merge and regular inner node
      switch(current_type) {
        case(NodeType::separator_node) :
          break;
        case(NodeType::split_node) :
          break;
        case(NodeType::inner_node) :
          const inner_node* current_inner = static_cast<const inner_node*>(current);
          int slot = find_lower(current_inner, key);
          current_pid = current_inner->child_pid[slot];
          current = mapping_table.get(current_pid);
          break;
      }
    }

    return current_pid;
  }

public:
  void insert_data(const DataPairType &x);
  void delete_key(const KeyType &x);
  void split_leaf(PID pid);
  bool exists(const KeyType &key);
  std::vector<std::pair<KeyType, ValueType>> search(const KeyType &key);
  std::vector<std::pair<KeyType, ValueType>> search_range(const KeyType &low_key, const KeyType &high_key);
  size_t count(const KeyType &key);

};

}  // End index namespace
}  // End peloton namespace
