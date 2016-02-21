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

#include <unordered_set>

#include "backend/index/bwtree.h"

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::insert_data(const DataPairType &x) {

  if (m_root == NULL_PID) {
    m_root = m_headleaf = m_tailleaf = allocate_leaf();
  }

  KeyType key = x.first;
  ValueType value = x.second;

  PID curr_pid = m_root;
  node *curr_node = get_node(m_root);

  while (!curr_node->is_leaf()) {
    while (curr_node->is_delta()) {
      curr_node = curr_node->get_node();
    }
    unsigned short slot = find_lower(curr_node, key);
    curr_pid = curr_node->child_pid[slot];
    curr_node = get_node(curr_pid);
  }

  // check whether the leaf node contains the key, need api

  insert_node *insert_delta = allocate_insert(x, curr_node);
  insert_delta->set_base(curr_node);
  set_node(curr_pid, insert_delta);
  if (insert_delta->is_full()) {
    split_leaf(curr_pid);
  }
}

template <typename KeyType, typename ValueType, class KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::update_data(const DataPairType &x) {

  if (m_root == NULL_PID) {
    m_root = m_headleaf = m_tailleaf = allocate_leaf();
  }

  KeyType key = x.first;
  ValueType value = x.second;

  PID curr_pid = m_root;
  node *curr_node = get_node(m_root);

  while (!curr_node->is_leaf()) {
    while (curr_node->is_delta()) {
      curr_node = curr_node->get_node();
    }
    unsigned short slot = find_lower(curr_node, key);
    curr_pid = curr_node->child_pid[slot];
    curr_node = get_node(curr_pid);
  }

  // check whether the leaf node contains the key, need api


  update_node *update_delta = allocate_update(x, curr_node);
  update_delta->set_base(curr_node);
  set_node(curr_pid, update_delta);

}

template <typename KeyType, typename ValueType, class KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::delete_key(const KeyType &x) {

  if (m_root == NULL_PID) {
    m_root = m_headleaf = m_tailleaf = allocate_leaf();
  }

  PID curr_pid = m_root;
  node *curr_node = get_node(m_root);

  while (!curr_node->is_leaf()) {
    while (curr_node->is_delta()) {
      curr_node = curr_node->get_node();
    }
    unsigned short slot = find_lower(curr_node, x);
    curr_pid = curr_node->child_pid[slot];
    curr_node = get_node(curr_pid);
  }
  
  // check whether the leaf node contains the key, need api

  delete_node *delete_delta = allocate_delete(x, curr_node, curr_pid);
  delete_delta->set_base(curr_node);
  set_node(curr_pid, delete_delta);
}

template <typename KeyType, typename ValueType, class KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::split_leaf(PID pid) {

  node *n = get_node(pid);
  node *base_node = get_base_node(n);

  PID former_next_leaf_pid = static_cast<leaf_node *>(base_node)->get_next();
  node *former_next_leaf = get_node(former_next_leaf_pid);

  std::vector<DataPairType> buffer = get_all_data(n);

  // split delta node
  KeyType split_key = buffer[buffer.size() / 2];

  PID next_leaf_pid = allocate_leaf();
  leaf_node *next_leaf = static_cast<leaf_node *>(get_node(next_leaf_pid));
  for (unsigned short slot = buffer.size() / 2; slot < buffer.size(); slot++) {
    next_leaf->set_slot(slot, buffer[slot]);
  }

  next_leaf->set_next(former_next_leaf_pid);
  next_leaf->set_prev(pid);

  n->set_next(next_leaf_pid);
  former_next_leaf->set_prev(next_leaf_pid);

  split_node *split_delta = allocate_split(split_key, next_leaf_pid, buffer.size() - buffer.size() / 2, n);
  split_delta->set_base(n);
  set_node(pid, split_delta);

  // separator delta node

  // create a inner node for root
  if (m_root == pid) {
    PID new_root = allocate_inner(1);
    base_node->set_parent(new_root);
    m_root = new_root;
  }

  PID parent_pid = base_node->get_parent();
  node *parent = get_node(parent_pid);
  node *parent_base_node = static_cast<inner_node *>(get_base_node(parent));
  unsigned short slot = find_lower(parent_base_node, split_key);
  KeyType right_key;
  if (slot >= parent_base_node->get_size())
    right_key = split_key;
  else
    right_key = parent_base_node->slot_key[slot];
  separator_node *separator_delta = allocate_separator(split_key, right_key, next_leaf_pid, parent);
  separator_delta->set_base(parent);
  set_node(parent_pid, separator_delta);
}


}  // End index namespace
}  // End peloton namespace
