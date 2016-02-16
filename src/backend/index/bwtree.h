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

#define BWTREE_MAX(a,b) ((a) < (b) ? (b) : (a))
#define BWTREE_NODE_SIZE 256

namespace peloton {
namespace index {

// Look up the stx btree interface for background.
// peloton/third_party/stx/btree.h
template <typename KeyType, typename ValueType, class KeyComparator>
class BWTree {

public:
  // *** Constructed Types

  typedef std::pair<KeyType, ValueType> PairType;

  typedef std::allocator<std::pair<KeyType, ValueType>> AllocType;

  typedef size_t PID;

public:
  // *** Static Constant Options and Values of the BW Tree

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

    leaf_node *prev_leaf;

    leaf_node *next_leaf;

    KeyType slot_key[leaf_slot_max];

    ValueType slot_data[leaf_slot_max];

    inline void initialize() {
      node::initialize(0, false);
      prev_leaf = next_leaf = NULL;
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
  };

  /// Extended structure of a delta node in memory. Contains a key, value
  /// pair to insert
  struct insert_node : public delta_node {
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

};

}  // End index namespace
}  // End peloton namespace
