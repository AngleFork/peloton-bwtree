//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bwtree.cpp
//
// Identification: src/backend/index/bwtree.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/index/bwtree.h"

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::insert_data(const PairType &x) {

  if (m_root == NULL_PID) {
      m_root = m_headleaf = m_tailleaf = allocate_leaf();
  }

  KeyType key = x.first;
  ValueType value = x.second;

  PID curr_pid = m_root;
  node *curr_node = mapping_table.get(m_root);

  while (!curr_node.is_leaf()) {
    unsigned short slot = find_lower(curr_node, key);
    curr_pid = curr_node->child_pid[slot];
    curr_node = mapping_table.get(curr_pid);
  }

  insert_node *delta = allocate_insert(0, x, curr_node);
  set_node(curr_pid, delta);

}

template <typename KeyType, typename ValueType, class KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::delete_key(const KeyType &x) {

  if (m_root == NULL_PID) {
      m_root = m_headleaf = m_tailleaf = allocate_leaf();
  }

  PID curr_pid = m_root;
  node *curr_node = mapping_table.get(m_root);

  while (!curr_node.is_leaf()) {
    unsigned short slot = find_lower(curr_node, x);
    curr_pid = curr_node->child_pid[slot];
    curr_node = mapping_table.get(curr_pid);
  }

  delete_node *delta = allocate_delete(0, x, curr_node);
  set_node(curr_pid, delta);

}


}  // End index namespace
}  // End peloton namespace
