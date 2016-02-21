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
#include "backend/common/types.h"
#include "backend/index/index_key.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::insert_data(__attribute__((unused)) const DataPairType &x) {

  if (m_root == NULL_PID) {
    m_root = m_headleaf = m_tailleaf = allocate_leaf();
  }

  KeyType key = x.first;

  PID curr_pid = m_root;
  node *curr_node = get_node(m_root);

  while (!curr_node->is_leaf()) {
    while (curr_node->is_delta()) {
      curr_node = static_cast<delta_node *>(curr_node)->get_base();
    }
    unsigned short slot = find_lower(static_cast<inner_node *>(curr_node), key);
    curr_pid = static_cast<inner_node *>(curr_node)->child_pid[slot];
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

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::update_data(const DataPairType &x) {

  if (m_root == NULL_PID) {
    m_root = m_headleaf = m_tailleaf = allocate_leaf();
  }

  KeyType key = x.first;

  PID curr_pid = m_root;
  node *curr_node = get_node(m_root);

  while (!curr_node->is_leaf()) {
    while (curr_node->is_delta()) {
      curr_node = static_cast<delta_node *>(curr_node)->get_base();
    }
    unsigned short slot = find_lower(static_cast<inner_node *>(curr_node), key);
    curr_pid = static_cast<inner_node *>(curr_node)->child_pid[slot];
    curr_node = get_node(curr_pid);
  }

  // check whether the leaf node contains the key, need api


  update_node *update_delta = allocate_update(x, curr_node);
  update_delta->set_base(curr_node);
  set_node(curr_pid, update_delta);

}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::delete_key(const KeyType &x) {

  if (m_root == NULL_PID) {
    m_root = m_headleaf = m_tailleaf = allocate_leaf();
  }

  PID curr_pid = m_root;
  node *curr_node = get_node(m_root);

  while (!curr_node->is_leaf()) {
    while (curr_node->is_delta()) {
      curr_node = static_cast<delta_node *>(curr_node)->get_base();
    }
    unsigned short slot = find_lower(static_cast<inner_node *>(curr_node), x);
    curr_pid = static_cast<inner_node *>(curr_node)->child_pid[slot];
    curr_node = get_node(curr_pid);
  }
  
  // check whether the leaf node contains the key, need api

  delete_node *delete_delta = allocate_delete(x, curr_node);
  delete_delta->set_base(curr_node);
  set_node(curr_pid, delete_delta);
}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::split_leaf(PID pid) {

  node *n = get_node(pid);
  leaf_node *base_node = static_cast<leaf_node *>(get_base_node(n));

  PID former_next_leaf_pid = static_cast<leaf_node *>(base_node)->get_next();
  leaf_node *former_next_leaf = static_cast<leaf_node *>(get_node(former_next_leaf_pid));

  std::vector<DataPairType> buffer = get_all_data(n);

  // split delta node
  unsigned short pos = static_cast<unsigned short>(buffer.size()) / 2;
  KeyType split_key = buffer[pos].first;

  PID next_leaf_pid = allocate_leaf();
  leaf_node *next_leaf = static_cast<leaf_node *>(get_node(next_leaf_pid));
  for (unsigned short slot = buffer.size() / 2; slot < buffer.size(); slot++) {
    next_leaf->set_slot(slot, buffer[slot]);
  }

  next_leaf->set_next(former_next_leaf_pid);
  next_leaf->set_prev(pid);

  base_node->set_next(next_leaf_pid);
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
  inner_node *parent_base_node = static_cast<inner_node *>(get_base_node(parent));
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

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::exists(const KeyType &key) {
  PID leaf_pid = get_leaf_node_pid(key);

  if(leaf_pid < 0) {
    return false;
  }

  node* leaf = mapping_table.get(leaf_pid);

  // Traversing delta chain here will be more efficient since we dont have to loop till base for some cases.
  // But for simplicity, we call get_all_data() and check if the key is in the returned vector.
  std::vector<DataPairType> node_data = get_all_data(leaf);

  return vector_contains_key(node_data, key);
}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
std::vector<std::pair<KeyType, ValueType>> BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::search(const KeyType &key) {
  std::vector<DataPairType> result;
  PID leaf_pid = get_leaf_node_pid(key);

  if(leaf_pid < 0) {
    return result;
  }

  // Find the leaf node and retrieve all records in the node
  node* leaf = mapping_table.get(leaf_pid);
  auto node_data = get_all_data(leaf);

  // Check if we have a match (possible improvement: implement binary search)
  for (auto it = node_data.begin() ; it != node_data.end(); ++it) {
    // For duplicate keys, there's an edge case that some records can be placed at the
    // next node so we need to check the next page as well.
    if(it != node_data.end() && (next(it) == node_data.end()) && key_equal(key, it->first)) {
      // TODO: handle duplicate keys
    }
    else if(key_equal(key, it->first)) {
      result.push_back(*it);
    }
  }
  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
std::vector<std::pair<KeyType, ValueType>> BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::search_range(const KeyType &low, const KeyType &high) {
  std::vector<DataPairType> result;
  PID low_pid = get_leaf_node_pid(low);
  PID high_pid = get_leaf_node_pid(high);

  if(low_pid < 0 || high_pid < 0) {
    return result;
  }

  // Case 1. low not exists  (e.g where x < 5)

  // Case 2. high not exists (e.g where x > 5)

  // Case 3. low & high both exist (e.g where x > 5 and x < 10)

  // const leaf_node* low_node = static_cast<const leaf_node*>(mapping_table.get(low_pid));
  // const leaf_node* high_node = static_cast<const leaf_node*>(mapping_table.get(high_pid));
  // PID current_pid = low_pid;

  return result;

}

template class BWTree<IntsKey<1>, ItemPointer, IntsComparator<1>,
        IntsEqualityChecker<1>>;
template class BWTree<IntsKey<2>, ItemPointer, IntsComparator<2>,
       IntsEqualityChecker<2>>;
template class BWTree<IntsKey<3>, ItemPointer, IntsComparator<3>,
       IntsEqualityChecker<3>>;
template class BWTree<IntsKey<4>, ItemPointer, IntsComparator<4>,
       IntsEqualityChecker<4>>;

template class BWTree<GenericKey<4>, ItemPointer, GenericComparator<4>,
       GenericEqualityChecker<4>>;
template class BWTree<GenericKey<8>, ItemPointer, GenericComparator<8>,
       GenericEqualityChecker<8>>;
template class BWTree<GenericKey<12>, ItemPointer, GenericComparator<12>,
       GenericEqualityChecker<12>>;
template class BWTree<GenericKey<16>, ItemPointer, GenericComparator<16>,
       GenericEqualityChecker<16>>;
template class BWTree<GenericKey<24>, ItemPointer, GenericComparator<24>,
       GenericEqualityChecker<24>>;
template class BWTree<GenericKey<32>, ItemPointer, GenericComparator<32>,
       GenericEqualityChecker<32>>;
template class BWTree<GenericKey<48>, ItemPointer, GenericComparator<48>,
       GenericEqualityChecker<48>>;
template class BWTree<GenericKey<64>, ItemPointer, GenericComparator<64>,
       GenericEqualityChecker<64>>;
template class BWTree<GenericKey<96>, ItemPointer, GenericComparator<96>,
       GenericEqualityChecker<96>>;
template class BWTree<GenericKey<128>, ItemPointer, GenericComparator<128>,
       GenericEqualityChecker<128>>;
template class BWTree<GenericKey<256>, ItemPointer, GenericComparator<256>,
       GenericEqualityChecker<256>>;
template class BWTree<GenericKey<512>, ItemPointer, GenericComparator<512>,
       GenericEqualityChecker<512>>;

template class BWTree<TupleKey, ItemPointer, TupleKeyComparator,
       TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace
