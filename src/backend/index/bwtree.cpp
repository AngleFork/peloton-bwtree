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
    LeafNode *leaf = AllocateLeaf();
    PID pid = AllocatePID();
    m_headleaf = pid;
    for (;;) {
      if (mapping_table.Update(pid, leaf, NULL, 0)) {
        break;
      }
    }
    m_root = m_headleaf = m_tailleaf = pid;
  }

  InsertNode *insert_delta;
  PID curr_pid;

  for (;;) {

    KeyType key = x.first;

    curr_pid = m_root;
    Node *curr_node = GetNode(m_root);

    while (!curr_node->IsLeaf()) {
      // while (curr_node->IsDelta()) {
      //   curr_node = static_cast<DeltaNode *>(curr_node)->GetBase();
      // }
      // unsigned short slot = FindLower(static_cast<InnerNode *>(curr_node), key);
      // curr_pid = static_cast<InnerNode *>(curr_node)->child_pid[slot];
      curr_pid = FindNextPID(curr_pid, key);
      curr_node = GetNode(curr_pid);
    }

    // check whether the leaf node contains the key, need api
    bool contain = LeafContainsKey(curr_node, x.first);

    insert_delta = AllocateInsert(x, curr_node->GetLevel());
    // SetNode(curr_pid, insert_delta);
    if (mapping_table.Update(curr_pid, insert_delta, curr_node, (contain) ? 0 : 1)) {
      LOG_INFO(" node size = %ld", insert_delta->GetSize());
      break;
    } else {
      FreeNode(insert_delta);
    }
  }
  LOG_INFO("insert is done on pid = %ld", curr_pid);

  if (insert_delta->IsFull()) {
    SplitLeaf(curr_pid);
  }

}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::UpdateData(const DataPairType &x) {

  if (m_root == NULL_PID) {
    LeafNode *leaf = AllocateLeaf();
    PID pid = AllocatePID();
    m_headleaf = pid;
    for (;;) {
      if (mapping_table.Update(pid, leaf, NULL, 0)) {
        break;
      }
    }
    m_root = m_headleaf = m_tailleaf = pid;
  }

  for (;;) {

    KeyType key = x.first;

    PID curr_pid = m_root;
    Node *curr_node = GetNode(m_root);

    while (!curr_node->IsLeaf()) {
      // while (curr_node->IsDelta()) {
      //   curr_node = static_cast<DeltaNode *>(curr_node)->GetBase();
      // }
      // unsigned short slot = FindLower(static_cast<InnerNode *>(curr_node), key);
      // curr_pid = static_cast<InnerNode *>(curr_node)->child_pid[slot];
      curr_pid = FindNextPID(curr_pid, key);
      curr_node = GetNode(curr_pid);
    }

    // check whether the leaf node contains the key, need api


    UpdateNode *update_delta = AllocateUpdate(x, curr_node->GetLevel());
    // SetNode(curr_pid, update_delta);
    if (mapping_table.Update(curr_pid, update_delta, curr_node, 0)) {
      break;
    } else {
      FreeNode(update_delta);
    }
  }

}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::DeleteKey(const KeyType &x) {
  LOG_INFO("delete key is called");

  if (m_root == NULL_PID) {
    LeafNode *leaf = AllocateLeaf();
    PID pid = AllocatePID();
    m_headleaf = pid;
    for (;;) {
      if (mapping_table.Update(pid, leaf, NULL, 0)) {
        break;
      }
    }
    m_root = m_headleaf = m_tailleaf = pid;
  }

  for (;;) {

    PID curr_pid = m_root;
    Node *curr_node = GetNode(m_root);

    while (!curr_node->IsLeaf()) {
      // while (curr_node->IsDelta()) {
      //   curr_node = static_cast<DeltaNode *>(curr_node)->GetBase();
      // }
      // unsigned short slot = FindLower(static_cast<InnerNode *>(curr_node), x);
      // curr_pid = static_cast<InnerNode *>(curr_node)->child_pid[slot];
      curr_pid = FindNextPID(curr_pid, x);
      curr_node = GetNode(curr_pid);
    }
    
    // check whether the leaf node contains the key, need api
    bool contain = LeafContainsKey(curr_node, x);
    if (!contain) {
      break;
    }

    DeleteNode *delete_delta = AllocateDeleteNoValue(x, curr_node->GetLevel());
    // SetNode(curr_pid, delete_delta);
    if (mapping_table.Update(curr_pid, delete_delta, curr_node, -1)) {
      break;
    } else {
      FreeNode(delete_delta);
    }
  }

  LOG_INFO("delete key is done");

}



template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::DeleteData(const DataPairType &x) {
  LOG_INFO("delete data is called");

  if (m_root == NULL_PID) {
    LeafNode *leaf = AllocateLeaf();
    PID pid = AllocatePID();
    m_headleaf = pid;
    for (;;) {
      if (mapping_table.Update(pid, leaf, NULL, 0)) {
        break;
      }
    }
    m_root = m_headleaf = m_tailleaf = pid;
  }

  for (;;) {

    KeyType key = x.first;

    PID curr_pid = m_root;
    Node *curr_node = GetNode(m_root);

    while (!curr_node->IsLeaf()) {
      // while (curr_node->IsDelta()) {
      //   curr_node = static_cast<DeltaNode *>(curr_node)->GetBase();
      // }
      // unsigned short slot = FindLower(static_cast<InnerNode *>(curr_node), key);
      // curr_pid = static_cast<InnerNode *>(curr_node)->child_pid[slot];
      curr_pid = FindNextPID(curr_pid, key);
      curr_node = GetNode(curr_pid);
    }
    
    // check whether the leaf node contains the key, need api

    DeleteNode *delete_delta = AllocateDeleteWithValue(x, curr_node->GetLevel());
    // SetNode(curr_pid, delete_delta);
    if (mapping_table.Update(curr_pid, delete_delta, curr_node, 0)){
      break;
    } else {
      FreeNode(delete_delta);
    }
  }
  LOG_INFO("delete data is done");

}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::SplitLeaf(PID pid) {

  LOG_INFO("split leaf is called on pid = %ld", pid);
  LeafNode *base_node;
  KeyType split_key;
  PID next_leaf_pid;
  PID parent_pid;


  // create a inner node for root
  if (m_root == pid) {
    base_node = static_cast<LeafNode *>(GetBaseNode(GetNode(pid)));

    InnerNode *inner = AllocateInner(1, pid);
    PID new_root = AllocatePID();

    base_node->SetParent(new_root);
    
    for (;;) {
      if (mapping_table.Update(new_root, inner, NULL, 0)) {
        break;
      }
    }
    m_root = new_root;
  }

  for (;;) {

    Node *n = GetNode(pid);
    if (!n->IsFull()) {
      return;
    }

    base_node = static_cast<LeafNode *>(GetBaseNode(n));
    parent_pid = base_node->GetParent();

    PID former_next_leaf_pid = static_cast<LeafNode *>(base_node)->GetNext();
    LeafNode *former_next_leaf = NULL;
    if (former_next_leaf_pid != NULL_PID)
      former_next_leaf = static_cast<LeafNode *>(GetBaseNode(GetNode(former_next_leaf_pid)));

    std::vector<DataPairListType> buffer = GetAllData(n);

    std::vector<DataPairType> result;
    for (auto it = buffer.begin() ; it != buffer.end(); ++it) {
      LOG_INFO(" key length = %d", it->second.GetSize());
      for (int i = 0; i < it->second.GetSize(); i++) {
        result.push_back(std::make_pair(it->first, it->second.GetValue(i)));
      }
    }
    LOG_INFO("result length = %ld", result.size());


    for (int i = 0; i < buffer.size() - 1; i++) {
      for (int j = i + 1; j < buffer.size(); j++) {
        if (KeyGreaterEqual(buffer[i].first, buffer[j].first)) {
          LOG_INFO("wrong order: %d %d", i, j);
        }
      }
    }

    // split delta node
    unsigned short pos = static_cast<unsigned short>(buffer.size()) / 2;
    split_key = buffer[pos].first;
    LOG_INFO("0 size = %ld, pos = %hu", buffer.size(), pos);

    LOG_INFO("1 pid = %ld", pid);

    LeafNode *next_leaf = AllocateLeaf();
    next_leaf_pid = AllocatePID();
    next_leaf->SetParent(parent_pid);
    for (;;) {
      if (mapping_table.Update(next_leaf_pid, next_leaf, NULL, 0)) {
        break;
      }
    }

    LOG_INFO("2 new pid = %ld", next_leaf_pid);
    for (unsigned short slot = buffer.size() / 2; slot < buffer.size(); slot++) {
      next_leaf->SetSlot(slot - buffer.size() / 2, buffer[slot]);
    }

    // next_leaf->SetNext(former_next_leaf_pid);
    // next_leaf->SetPrev(pid);

    // LOG_INFO("3 former_next_leaf_pid = %ld", former_next_leaf_pid);
    // base_node->SetNext(next_leaf_pid);

    // LOG_INFO("4");
    // if (former_next_leaf_pid != NULL_PID) {
    //   former_next_leaf->SetPrev(next_leaf_pid);
    // }

    LOG_INFO("5");

    SplitNode *split_delta = AllocateSplit(split_key, next_leaf_pid, n->GetLevel());
    LOG_INFO("6");
    if (mapping_table.Update(pid, split_delta, n, buffer.size() / 2)) {


      next_leaf->SetNext(former_next_leaf_pid);
      next_leaf->SetPrev(pid);

      LOG_INFO("3 former_next_leaf_pid = %ld", former_next_leaf_pid);
      base_node->SetNext(next_leaf_pid);

      LOG_INFO("4");
      if (former_next_leaf_pid != NULL_PID) {
        former_next_leaf->SetPrev(next_leaf_pid);
      }

      LOG_INFO("left  size = %ld", split_delta->GetSize());
      LOG_INFO("right size = %ld", next_leaf->GetSize());
      break;
    } else {
      FreeNode(next_leaf);
      FreeNode(split_delta);
      // base_node->SetNext(former_next_leaf_pid);
      // if (former_next_leaf_pid != NULL_PID) {
      //   former_next_leaf->SetPrev(pid);
      // }
      LOG_INFO("split delta failed");
    }
  }

  LOG_INFO("split delta is added");





  // separator delta node
  for (;;) {
    Node *parent = GetNode(parent_pid);
    KeyType right_key = FindUpperKey(parent_pid, split_key);
    if (KeyEqual(split_key, right_key)) {
      LOG_INFO("separator_delta is right most");
    } else {
      LOG_INFO("separator_delata is not right most");
    }

    SeparatorNode *separator_delta = AllocateSeparator(split_key, right_key, next_leaf_pid, parent->GetLevel());

    if (mapping_table.Update(parent_pid, separator_delta, parent, 1)) {
      break;
    } else {
      FreeNode(separator_delta);
    }
  }

  LOG_INFO("index entry is added");

  LOG_INFO("split leaf is done");
}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Exists(const KeyType &key) {
  PID leaf_pid = GetLeafNodePID(key);

  if (leaf_pid < 0) {
    return false;
  }

  Node* leaf = mapping_table.Get(leaf_pid);

  // Traversing delta chain here will be more efficient since we dont have to loop till base for some cases.
  // But for simplicity, we call get_all_data() and check if the key is in the returned vector.
  std::vector<DataPairListType> node_data = GetAllData(leaf);

  return VectorContainsKey2(node_data, key);
}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
std::vector<std::pair<KeyType, ValueType>> BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Search(const KeyType &key) {
  std::vector<DataPairType> result;
  PID leaf_pid = GetLeafNodePID(key);
  LOG_INFO("search is called");
  if(leaf_pid == NULL_PID) {
    return result;
  }

  // Find the leaf node and retrieve all records in the node
  Node* leaf = mapping_table.Get(leaf_pid);
  if(leaf->IsDelta()) {
    size_t delta_length = static_cast<DeltaNode*>(leaf)->GetLength();
    if(delta_length > DELTA_THRESHOLD) {
      ConsolidateLeafNode(leaf_pid);
    }
  }

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

      for (int i = 0; i < it->second.GetSize(); i++) {
        result.push_back(std::make_pair(it->first, it->second.GetValue(i)));
      }
    }
  }
  LOG_INFO("search is done");
  return result;
}


template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
std::vector<std::pair<KeyType, ValueType>> BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::SearchAll() {
  std::vector<DataPairType> result;
  LOG_INFO("search all is called");

  // Find the leaf node and retrieve all records in the node
  PID leaf_pid = m_headleaf;
  Node* leaf = mapping_table.Get(leaf_pid);
  while (leaf_pid != NULL_PID) {
    LOG_INFO("search for leaf pid = %ld", leaf_pid);
    if (leaf->IsDelta()) {
      LOG_INFO("length = %ld", static_cast<DeltaNode *>(leaf)->GetLength());
    }
    LOG_INFO("node size = %ld", leaf->GetSize());
    auto node_data = GetAllData(leaf);

    LOG_INFO("search done for leaf pid = %ld, size = %ld", leaf_pid, node_data.size());
    // Check if we have a match (possible improvement: implement binary search)
    for (auto it = node_data.begin() ; it != node_data.end(); ++it) {
      LOG_INFO(" key length = %d", it->second.GetSize());
      for (int i = 0; i < it->second.GetSize(); i++) {
        result.push_back(std::make_pair(it->first, it->second.GetValue(i)));
      }
      // result.push_back(*it);
    }

    LOG_INFO("result length = %ld", result.size());

    leaf_pid = static_cast<LeafNode *>(GetBaseNode(leaf))->GetNext();
    if (leaf_pid != NULL_PID) {
      leaf = mapping_table.Get(leaf_pid);
    }
  }
  LOG_INFO("search all is done");
  return result;
}

//template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
//std::vector<std::pair<KeyType, ValueType>> BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::SearchRange(const KeyType &low, const KeyType &high) {
//  std::vector<DataPairType> result;
//  PID low_pid = GetLeafNodePID(low);
//  PID high_pid = GetLeafNodePID(high);
//
//  if(low_pid < 0 || high_pid < 0) {
//    return result;
//  }
//
//  // Case 1. low not exists  (e.g where x < 5)
//
//  // Case 2. high not exists (e.g where x > 5)
//
//  // Case 3. low & high both exist (e.g where x > 5 and x < 10)
//
//  // const leaf_node* low_node = static_cast<const leaf_node*>(mapping_table.get(low_pid));
//  // const leaf_node* high_node = static_cast<const leaf_node*>(mapping_table.get(high_pid));
//  // PID current_pid = low_pid;
//
//  return result;
//
//}


//template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
//void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ConsolidateInnerNode(
//  PID pid) {
//
//  // This node must be delta node since we are calling consolidation when the threshold for delta length exceeds
//  Node* node = mapping_table.Get(pid);
//
//  NodeType type = node->GetType();
//
//  unsigned short level;
//  PID child;
//
//  InnerNode* inner = AllocateInner(level, child);
//
//  switch(node->GetType()) {
//    case NodeType::separator_node:
//    case NodeType::split_node:
//      break;
//  }
//
//
//
//  inner->child_pid
//  inner->level
//  inner->parent
//  inner->slot_key
//  inner->slot_use
//
//}



template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ConsolidateLeafNode(
  PID pid) {
  for(;;) {
    LOG_INFO("CONSOLIDATION STARTS!");

    // This node must be delta node since we are calling consolidation when the threshold for delta length exceeds
    Node* old = mapping_table.Get(pid);

    Node* node = old;
    LeafNode* consolidated = AllocateLeaf();

    consolidated->next_leaf = NULL_PID;
    consolidated->prev_leaf = NULL_PID;

    // Set parent, level information
    consolidated->parent = node->parent;
    consolidated->level = node->level;

    // Get the base node
    if(node->IsDelta()) {
      node = static_cast<DeltaNode *>(node)->GetBase();
    }

    consolidated->next_leaf = static_cast<LeafNode*>(node)->next_leaf;
    consolidated->prev_leaf = static_cast<LeafNode*>(node)->prev_leaf;

    // Set the key slot, data.
    auto data = GetAllData(node);
    consolidated->slot_use = data.size();

    int index = 0;
    for (auto it = data.begin() ; it != data.end(); ++it) {
      consolidated->slot_key[index] = it->first;
      for(int i = 0; i < it->second.GetSize(); i++) {
        consolidated->slot_data[index].InsertValue(it->second.GetValue(i));
      }

//      consolidated->slot_data[index] = it->second;
      index++;
    }

    LOG_INFO("consolidated node next_leaf(%ld), prev_leaf(%ld), parent(%ld)", consolidated->next_leaf, consolidated->prev_leaf, consolidated->parent);

    // Check if there was any change in the mapping table while consolidating
    if(mapping_table.Update(pid, consolidated, old, 0)){
      break;
    }

    epoch_table.RegisterNode(old);
  }
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
