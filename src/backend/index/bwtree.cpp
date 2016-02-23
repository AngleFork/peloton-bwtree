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
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::InsertData(__attribute__((unused)) const DataPairType &x) {
  LOG_INFO("insert is called");

  if (m_root == NULL_PID) {
    m_root = m_headleaf = m_tailleaf = AllocateLeaf();
  }

  KeyType key = x.first;

  PID curr_pid = m_root;
  Node *curr_node = GetNode(m_root);

  while (!curr_node->IsLeaf()) {
    while (curr_node->IsDelta()) {
      curr_node = static_cast<DeltaNode *>(curr_node)->GetBase();
    }
    unsigned short slot = FindLower(static_cast<InnerNode *>(curr_node), key);
    curr_pid = static_cast<InnerNode *>(curr_node)->child_pid[slot];
    curr_node = GetNode(curr_pid);
  }

  // check whether the leaf node contains the key, need api

  InsertNode *insert_delta = AllocateInsert(x, curr_node);
  SetNode(curr_pid, insert_delta);
  // if (insert_delta->is_full()) {
  //   split_leaf(curr_pid);
  // }
  LOG_INFO("insert is done");

}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::UpdateData(const DataPairType &x) {

  if (m_root == NULL_PID) {
    m_root = m_headleaf = m_tailleaf = AllocateLeaf();
  }

  KeyType key = x.first;

  PID curr_pid = m_root;
  Node *curr_node = GetNode(m_root);

  while (!curr_node->IsLeaf()) {
    while (curr_node->IsDelta()) {
      curr_node = static_cast<DeltaNode *>(curr_node)->GetBase();
    }
    unsigned short slot = FindLower(static_cast<InnerNode *>(curr_node), key);
    curr_pid = static_cast<InnerNode *>(curr_node)->child_pid[slot];
    curr_node = GetNode(curr_pid);
  }

  // check whether the leaf node contains the key, need api


  UpdateNode *update_delta = allocate_update(x, curr_node);
  SetNode(curr_pid, update_delta);

}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::DeleteKey(const KeyType &x) {
  LOG_INFO("delete key is called");

  if (m_root == NULL_PID) {
    m_root = m_headleaf = m_tailleaf = AllocateLeaf();
  }

  PID curr_pid = m_root;
  Node *curr_node = GetNode(m_root);

  while (!curr_node->IsLeaf()) {
    while (curr_node->IsDelta()) {
      curr_node = static_cast<DeltaNode *>(curr_node)->GetBase();
    }
    unsigned short slot = FindLower(static_cast<InnerNode *>(curr_node), x);
    curr_pid = static_cast<InnerNode *>(curr_node)->child_pid[slot];
    curr_node = GetNode(curr_pid);
  }
  
  // check whether the leaf node contains the key, need api

  DeleteNode *delete_delta = AllocateDeleteNoValue(x, curr_node);
  SetNode(curr_pid, delete_delta);
  LOG_INFO("delete key is done");

}



template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::DeleteData(const DataPairType &x) {
  LOG_INFO("delete data is called");

  if (m_root == NULL_PID) {
    m_root = m_headleaf = m_tailleaf = AllocateLeaf();
  }

  KeyType key = x.first;

  PID curr_pid = m_root;
  Node *curr_node = GetNode(m_root);

  while (!curr_node->IsLeaf()) {
    while (curr_node->IsDelta()) {
      curr_node = static_cast<DeltaNode *>(curr_node)->GetBase();
    }
    unsigned short slot = FindLower(static_cast<InnerNode *>(curr_node), key);
    curr_pid = static_cast<InnerNode *>(curr_node)->child_pid[slot];
    curr_node = GetNode(curr_pid);
  }
  
  // check whether the leaf node contains the key, need api

  DeleteNode *delete_delta = allocate_delete_with_value(x, curr_node);
  SetNode(curr_pid, delete_delta);
  LOG_INFO("delete data is done");

}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::SplitLeaf(PID pid) {

  Node *n = GetNode(pid);
  LeafNode *base_node = static_cast<LeafNode *>(GetBaseNode(n));

  PID former_next_leaf_pid = static_cast<LeafNode *>(base_node)->GetNext();
  LeafNode *former_next_leaf = static_cast<LeafNode *>(GetNode(former_next_leaf_pid));

  std::vector<DataPairType> buffer = GetAllData(n);

  // split delta node
  unsigned short pos = static_cast<unsigned short>(buffer.size()) / 2;
  KeyType split_key = buffer[pos].first;

  PID next_leaf_pid = AllocateLeaf();
  LeafNode *next_leaf = static_cast<LeafNode *>(GetNode(next_leaf_pid));
  for (unsigned short slot = buffer.size() / 2; slot < buffer.size(); slot++) {
    next_leaf->SetSlot(slot, buffer[slot]);
  }

  next_leaf->SetNext(former_next_leaf_pid);
  next_leaf->SetPrev(pid);

  base_node->SetNext(next_leaf_pid);
  former_next_leaf->SetPrev(next_leaf_pid);

  SplitNode *split_delta = allocate_split(split_key, next_leaf_pid, buffer.size() - buffer.size() / 2, n);
  split_delta->SetBase(n);
  SetNode(pid, split_delta);

  // separator delta node

  // create a inner node for root
  if (m_root == pid) {
    PID new_root = AllocateInner(1);
    base_node->SetParent(new_root);
    m_root = new_root;
  }

  PID parent_pid = base_node->GetParent();
  Node *parent = GetNode(parent_pid);
  InnerNode *parent_base_node = static_cast<InnerNode *>(GetBaseNode(parent));
  unsigned short slot = FindLower(parent_base_node, split_key);
  KeyType right_key;
  if (slot >= parent_base_node->GetSize())
    right_key = split_key;
  else
    right_key = parent_base_node->slot_key[slot];
  SeparatorNode *separator_delta = allocate_separator(split_key, right_key, next_leaf_pid, parent);
  separator_delta->SetBase(parent);
  SetNode(parent_pid, separator_delta);
}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Exists(const KeyType &key) {
  PID leaf_pid = GetLeafNodePID(key);

  if(leaf_pid < 0) {
    return false;
  }

  Node* leaf = mapping_table.get(leaf_pid);

  // Traversing delta chain here will be more efficient since we dont have to loop till base for some cases.
  // But for simplicity, we call get_all_data() and check if the key is in the returned vector.
  std::vector<DataPairType> node_data = GetAllData(leaf);

  return VectorContainsKey(node_data, key);
}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
std::vector<std::pair<KeyType, ValueType>> BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Search(const KeyType &key) {
  std::vector<DataPairType> result;
  PID leaf_pid = GetLeafNodePID(key);
  LOG_INFO("search is called");
  if(leaf_pid < 0) {
    return result;
  }

  // Find the leaf node and retrieve all records in the node
  Node* leaf = mapping_table.get(leaf_pid);
  auto node_data = GetAllData(leaf);
  LOG_INFO("get_all_data is done");

  // Check if we have a match (possible improvement: implement binary search)
  for (auto it = node_data.begin() ; it != node_data.end(); ++it) {
    // For duplicate keys, there's an edge case that some records can be placed at the
    // next node so we need to check the next page as well.
//    if(it != node_data.end() && (next(it) == node_data.end()) && key_equal(key, it->first)) {
//      // TODO: handle duplicate keys
//    }
    if(KeyEqual(key, it->first)) {
      LOG_INFO("key match found");
      result.push_back(*it);
    }
  }
  LOG_INFO("search is done");
  return result;
}


template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
std::vector<std::pair<KeyType, ValueType>> BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::SearchAll() {
  std::vector<DataPairType> result;
  PID leaf_pid = m_root;
  LOG_INFO("search all is called");
  if(leaf_pid < 0) {
    return result;
  }

  // Find the leaf node and retrieve all records in the node
  Node* leaf = mapping_table.get(leaf_pid);
  auto node_data = GetAllData(leaf);
  LOG_INFO("get_all_data is done");

  // Check if we have a match (possible improvement: implement binary search)
  for (auto it = node_data.begin() ; it != node_data.end(); ++it) {
    // For duplicate keys, there's an edge case that some records can be placed at the
    // next node so we need to check the next page as well.
//    if(it != node_data.end() && (next(it) == node_data.end()) && key_equal(key, it->first)) {
//      // TODO: handle duplicate keys
//    }
    result.push_back(*it);
  }
  LOG_INFO("search all is done");
  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
std::vector<std::pair<KeyType, ValueType>> BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::SearchRange(const KeyType &low, const KeyType &high) {
  std::vector<DataPairType> result;
  PID low_pid = GetLeafNodePID(low);
  PID high_pid = GetLeafNodePID(high);

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

// Debug Purpose
template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Print() {
  LOG_INFO("bw tree print");

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
