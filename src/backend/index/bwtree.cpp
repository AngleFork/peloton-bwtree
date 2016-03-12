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

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::InsertData(
    __attribute__((unused)) const DataPairType &x) {
  // LOG_INFO("insert is called");

  // for (;;) {
  //   Node *root_node = GetNode(m_root);
  //   if (root_node != NULL) {
  //     break;
  //   }

  //   LeafNode *leaf = AllocateLeaf();
  //   if (mapping_table.Update(m_root, leaf, root_node)) {
  //     m_headleaf = m_root;
  //     break;
  //   } else{
  //     FreeNode(leaf);
  //   }
  // }

  if (m_root == NULL_PID) {
    LeafNode *leaf = AllocateLeaf();
    PID pid;
    for (;;) {
      pid = AllocatePID();
      if (mapping_table.Update(pid, leaf, NULL)) {
        break;
      }
    }
    if (__sync_bool_compare_and_swap(&m_root, NULL_PID, pid) == true) {
      m_headleaf = m_tailleaf = pid;
    } else {
      FreeNode(leaf);
    }
    // if (m_root == NULL_PID) {
    //   m_root = m_headleaf = m_tailleaf = pid;
    // } else {
    //   FreeNode(leaf);
    // }
  }

  InsertNode *insert_delta;
  PID curr_pid;

  for (;;) {
    KeyType key = x.first;

    curr_pid = m_root;
    Node *curr_node = GetNode(m_root);

    // LOG_INFO("insert into %ld 0", curr_pid);
    while (!curr_node->IsLeaf()) {
      // LOG_INFO("insert next of %ld 1", curr_pid);
      curr_pid = FindNextPID(curr_pid, key);
      // LOG_INFO("insert into %ld 1", curr_pid);
      curr_node = GetNode(curr_pid);
    }

    // check whether the leaf node contains the key, need api
    PID prev_pid;
    for (;;) {
      if (isInRange(curr_node, x.first)) break;
      // LOG_INFO(" not here at %ld", curr_pid);
      prev_pid = curr_pid;
      curr_pid = static_cast<LeafNode *>(GetBaseNode(curr_node))->GetNext();
      curr_node = GetNode(curr_pid);
      // LOG_INFO("insert into %ld 2", curr_pid);
      if (curr_node == NULL) {
        // LOG_INFO(" pid = %ld is null", curr_pid);
        curr_pid = prev_pid;
        curr_node = GetNode(curr_pid);
      }
    }

    // LOG_INFO("insert into %ld final", curr_pid);

    // allocate a insert delta
    insert_delta = AllocateInsert(x, curr_node->GetLevel());
    insert_delta->SetBase(curr_node);
    if (curr_node->IsDelta()) {
      insert_delta->SetLength(static_cast<DeltaNode *>(curr_node)->GetLength() +
                              1);
    } else {
      insert_delta->SetLength(1);
    }
    insert_delta->SetSize(((LeafContainsKey(curr_node, x.first) == 1) ? 0 : 1) +
                          curr_node->GetSize());

    // CAS
    if (mapping_table.Update(curr_pid, insert_delta, curr_node)) {
      // LOG_INFO("insert into %ld succeed", curr_pid);
      // LOG_INFO(" node size = %ld, node length = %ld",
      // insert_delta->GetSize(), insert_delta->GetLength());
      if (insert_delta->IsLeafFull()) {
        SplitLeaf(curr_pid);
      }
      break;
    } else {
      // LOG_INFO("insert into %ld fail", curr_pid);
      FreeNode(insert_delta);
    }
  }
  // LOG_INFO("insert is done on pid = %ld", curr_pid);
  if (GetNode(curr_pid) == NULL) {
    // LOG_INFO("after insert is done on pid = %ld, node is null", curr_pid);
  } else {
    // LOG_INFO("after insert is done on pid = %ld, node is not null",
    // curr_pid);
  }

  // LOG_INFO(" size = %ld", result.size());
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::UpdateData(
    const DataPairType &x) {
  if (m_root == NULL_PID) {
    LeafNode *leaf = AllocateLeaf();
    PID pid;
    for (;;) {
      pid = AllocatePID();
      if (mapping_table.Update(pid, leaf, NULL)) {
        break;
      }
    }
    if (__sync_bool_compare_and_swap(&m_root, NULL_PID, pid) == true) {
      m_headleaf = m_tailleaf = pid;
    } else {
      FreeNode(leaf);
    }
    // if (m_root == NULL_PID) {
    //   m_root = m_headleaf = m_tailleaf = pid;
    // } else {
    //   FreeNode(leaf);
    // }
  }

  for (;;) {
    KeyType key = x.first;

    PID curr_pid = m_root;
    Node *curr_node = GetNode(m_root);

    while (!curr_node->IsLeaf()) {
      curr_pid = FindNextPID(curr_pid, key);
      curr_node = GetNode(curr_pid);
    }

    // check whether the leaf node contains the key, need api
    for (;;) {
      if (isInRange(curr_node, x.first)) break;
      curr_pid = static_cast<LeafNode *>(GetBaseNode(curr_node))->GetNext();
      curr_node = GetNode(curr_pid);
    }
    if (!LeafContainsKey(curr_node, x.first)) {
      break;
    }

    UpdateNode *update_delta = AllocateUpdate(x, curr_node->GetLevel());
    update_delta->SetBase(curr_node);
    if (curr_node->IsDelta()) {
      update_delta->SetLength(static_cast<DeltaNode *>(curr_node)->GetLength() +
                              1);
    } else {
      update_delta->SetLength(1);
    }
    update_delta->SetSize(curr_node->GetSize());

    if (mapping_table.Update(curr_pid, update_delta, curr_node)) {
      break;
    } else {
      FreeNode(update_delta);
    }
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::DeleteKey(
    const KeyType &x) {
  // LOG_INFO("delete key is called");

  if (m_root == NULL_PID) {
    LeafNode *leaf = AllocateLeaf();
    PID pid;
    for (;;) {
      pid = AllocatePID();
      if (mapping_table.Update(pid, leaf, NULL)) {
        break;
      }
    }
    if (__sync_bool_compare_and_swap(&m_root, NULL_PID, pid) == true) {
      m_headleaf = m_tailleaf = pid;
    } else {
      FreeNode(leaf);
    }
    // if (m_root == NULL_PID) {
    //   m_root = m_headleaf = m_tailleaf = pid;
    // } else {
    //   FreeNode(leaf);
    // }
  }

  for (;;) {
    PID curr_pid = m_root;
    Node *curr_node = GetNode(m_root);

    while (!curr_node->IsLeaf()) {
      curr_pid = FindNextPID(curr_pid, x);
      curr_node = GetNode(curr_pid);
    }

    // check whether the leaf node contains the key, need api
    for (;;) {
      if (isInRange(curr_node, x)) break;
      curr_pid = static_cast<LeafNode *>(GetBaseNode(curr_node))->GetNext();
      curr_node = GetNode(curr_pid);
    }

    DeleteNode *delete_delta = AllocateDeleteNoValue(x, curr_node->GetLevel());
    delete_delta->SetBase(curr_node);
    if (curr_node->IsDelta()) {
      delete_delta->SetLength(static_cast<DeltaNode *>(curr_node)->GetLength() +
                              1);
    } else {
      delete_delta->SetLength(1);
    }
    delete_delta->SetSize(curr_node->GetSize());

    if (mapping_table.Update(curr_pid, delete_delta, curr_node)) {
      break;
    } else {
      FreeNode(delete_delta);
    }
  }

  // LOG_INFO("delete key is done");
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::DeleteData(
    const DataPairType &x) {
  // LOG_INFO("delete data is called");

  if (m_root == NULL_PID) {
    LeafNode *leaf = AllocateLeaf();
    PID pid;
    for (;;) {
      pid = AllocatePID();
      if (mapping_table.Update(pid, leaf, NULL)) {
        break;
      }
    }
    if (__sync_bool_compare_and_swap(&m_root, NULL_PID, pid) == true) {
      m_headleaf = m_tailleaf = pid;
    } else {
      FreeNode(leaf);
    }
    // if (m_root == NULL_PID) {
    //   m_root = m_headleaf = m_tailleaf = pid;
    // } else {
    //   FreeNode(leaf);
    // }
  }

  for (;;) {
    KeyType key = x.first;

    PID curr_pid = m_root;
    Node *curr_node = GetNode(m_root);

    while (!curr_node->IsLeaf()) {
      curr_pid = FindNextPID(curr_pid, key);
      curr_node = GetNode(curr_pid);
    }

    // check whether the leaf node contains the key, need api
    for (;;) {
      if (isInRange(curr_node, x.first)) break;
      curr_pid = static_cast<LeafNode *>(GetBaseNode(curr_node))->GetNext();
      curr_node = GetNode(curr_pid);
    }

    DeleteNode *delete_delta =
        AllocateDeleteWithValue(x, curr_node->GetLevel());
    delete_delta->SetBase(curr_node);
    if (curr_node->IsDelta()) {
      delete_delta->SetLength(static_cast<DeltaNode *>(curr_node)->GetLength() +
                              1);
    } else {
      delete_delta->SetLength(1);
    }
    delete_delta->SetSize(curr_node->GetSize());

    if (mapping_table.Update(curr_pid, delete_delta, curr_node)) {
      break;
    } else {
      FreeNode(delete_delta);
    }
  }
  // LOG_INFO("delete data is done");
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::SplitLeaf(
    PID pid) {
  // LOG_INFO("split leaf is called on pid = %ld", pid);
  LeafNode *base_node;
  KeyType split_key;
  PID next_leaf_pid;
  PID parent_pid;

  // create a inner node for root
  if (m_root == pid) {
    base_node = static_cast<LeafNode *>(GetBaseNode(GetNode(pid)));

    InnerNode *inner = AllocateInner(1, pid);
    PID new_root;

    for (;;) {
      new_root = AllocatePID();
      // LOG_INFO("new root = %ld", new_root);
      if (mapping_table.Update(new_root, inner, NULL)) {
        break;
      }
    }
    if (__sync_bool_compare_and_swap(&m_root, pid, new_root) == true) {
      base_node->SetParent(new_root);
      m_root = new_root;
    } else {
      FreeNode(inner);
    }
    // if (m_root == pid) {
    //   base_node->SetParent(new_root);
    //   m_root = new_root;
    // } else {
    //   FreeNode(inner);
    // }
  }

  for (;;) {
    Node *n = GetNode(pid);
    if (!n->IsLeafFull()) {
      return;
    }
    // LOG_INFO("split pid = %ld, chain length = %ld", pid,
    // static_cast<DeltaNode *>(n)->GetLength());

    base_node = static_cast<LeafNode *>(GetBaseNode(n));
    parent_pid = base_node->GetParent();
    // LOG_INFO("split pid = %ld, parent = %ld", pid, parent_pid);

    PID former_next_leaf_pid = static_cast<LeafNode *>(base_node)->GetNext();
    LeafNode *former_next_leaf = NULL;
    if (former_next_leaf_pid != NULL_PID)
      former_next_leaf =
          static_cast<LeafNode *>(GetBaseNode(GetNode(former_next_leaf_pid)));

    std::vector<DataPairListType> buffer = GetAllData(n);

    std::vector<DataPairType> result;
    for (auto it = buffer.begin(); it != buffer.end(); ++it) {
      // LOG_INFO("split pid = %ld, key length = %d", pid,
      // it->second.GetSize());
      for (int i = 0; i < it->second.GetSize(); i++) {
        result.push_back(std::make_pair(it->first, it->second.GetValue(i)));
      }
    }
    // LOG_INFO("split pid = %ld, result length = %ld", pid, result.size());

    // split delta node
    unsigned short pos = static_cast<unsigned short>(buffer.size()) / 2;
    split_key = buffer[pos].first;
    // LOG_INFO("split pid = %ld, 0 size = %ld, pos = %hu", pid, buffer.size(),
    // pos);

    LeafNode *next_leaf = AllocateLeaf();
    next_leaf->SetParent(parent_pid);
    for (;;) {
      next_leaf_pid = AllocatePID();
      if (mapping_table.Update(next_leaf_pid, next_leaf, NULL)) {
        break;
      }
    }

    // LOG_INFO("split pid = %ld, new pid = %ld", pid, next_leaf_pid);
    for (unsigned short slot = buffer.size() / 2; slot < buffer.size();
         slot++) {
      next_leaf->SetSlot(slot - buffer.size() / 2, buffer[slot]);
    }

    SplitNode *split_delta =
        AllocateSplit(split_key, next_leaf_pid, n->GetLevel());
    split_delta->SetBase(n);
    if (n->IsDelta()) {
      split_delta->SetLength(static_cast<DeltaNode *>(n)->GetLength() + 1);
    } else {
      split_delta->SetLength(1);
    }
    split_delta->SetSize(buffer.size() / 2);

    if (mapping_table.Update(pid, split_delta, n)) {
      base_node->SetNext(next_leaf_pid);

      next_leaf->SetNext(former_next_leaf_pid);
      next_leaf->SetPrev(pid);

      if (former_next_leaf_pid != NULL_PID) {
        former_next_leaf->SetPrev(next_leaf_pid);
      }

      // LOG_INFO("split pid = %ld, left  size = %ld", pid,
      // split_delta->GetSize());
      // LOG_INFO("split pid = %ld, right size = %ld", pid,
      // next_leaf->GetSize());
      break;
    } else {
      FreeNode(next_leaf);
      FreeNode(split_delta);
      // LOG_INFO("split pid = %ld, split delta failed", pid);
    }
  }

  // LOG_INFO("split pid = %ld, split delta is added", pid);
  // LOG_INFO("split pid = %ld, next leaf is %ld", pid, next_leaf_pid);
  if (GetNode(next_leaf_pid) == NULL) {
    // LOG_INFO("split pid = %ld, next leaf null", pid);
  } else {
    // LOG_INFO("split pid = %ld, next leaf not null", pid);
  }

  // separator delta node
  for (;;) {
    // LOG_INFO("split pid = %ld, add index entry to parent %ld", pid,
    // parent_pid);
    Node *parent = GetNode(parent_pid);
    KeyType right_key = FindUpperKey(parent_pid, split_key);

    SeparatorNode *separator_delta = AllocateSeparator(
        split_key, right_key, next_leaf_pid, parent->GetLevel());
    separator_delta->SetBase(parent);
    if (parent->IsDelta()) {
      separator_delta->SetLength(static_cast<DeltaNode *>(parent)->GetLength() +
                                 1);
    } else {
      separator_delta->SetLength(1);
    }
    separator_delta->SetSize(1 + parent->GetSize());
    // LOG_INFO("after add, size = %ld", separator_delta->GetSize());

    if (mapping_table.Update(parent_pid, separator_delta, parent)) {
      if (separator_delta->IsInnerFull()) {
        SplitInner(parent_pid);
      }
      break;
    } else {
      separator_delta->child = NULL_PID;
      FreeNode(separator_delta);
    }
  }

  // LOG_INFO("split pid = %ld, index entry is added", pid);

  // LOG_INFO("split pid = %ld, split leaf is done", pid);
  if (GetNode(pid) == NULL) {
    // LOG_INFO("split pid = %ld, node null", pid);
  } else {
    // LOG_INFO("split pid = %ld, node not null", pid);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::SplitInner(
    PID pid) {
  // LOG_INFO("max inner size = %hu", inner_slot_max);
  // LOG_INFO("split inner is called on pid = %ld", pid);
  InnerNode *base_node;
  KeyType split_key;
  PID next_inner_pid;
  PID parent_pid;

  // create a inner node for root
  if (m_root == pid) {
    base_node = static_cast<InnerNode *>(GetBaseNode(GetNode(pid)));

    InnerNode *inner = AllocateInner(base_node->GetLevel() + 1, pid);
    PID new_root;

    for (;;) {
      new_root = AllocatePID();
      // LOG_INFO("new root = %ld", new_root);
      if (mapping_table.Update(new_root, inner, NULL)) {
        break;
      }
    }
    if (__sync_bool_compare_and_swap(&m_root, pid, new_root) == true) {
      base_node->SetParent(new_root);
      m_root = new_root;
    } else {
      FreeNode(inner);
    }
    // if (m_root == pid) {
    //   base_node->SetParent(new_root);
    //   m_root = new_root;
    // } else {
    //   FreeNode(inner);
    // }
  }

  for (;;) {
    Node *n = GetNode(pid);
    // LOG_INFO(" size = %ld", n->GetSize());
    if (!n->IsInnerFull()) {
      return;
    }

    base_node = static_cast<InnerNode *>(GetBaseNode(n));
    parent_pid = base_node->GetParent();
    // LOG_INFO("split pid = %ld, parent = %ld", pid, parent_pid);

    PID former_next_inner_pid = static_cast<InnerNode *>(base_node)->GetNext();

    std::vector<PointerPairType> buffer = GetAllPointer(n);
    // LOG_INFO(" buffer size = %ld", buffer.size());

    // split delta node
    unsigned short num_key = static_cast<unsigned short>(buffer.size());
    unsigned short pos = num_key / 2;
    split_key = buffer[pos].first;

    InnerNode *next_inner = AllocateInner(n->GetLevel(), buffer[pos].second);

    next_inner->SetParent(parent_pid);
    for (;;) {
      next_inner_pid = AllocatePID();
      if (mapping_table.Update(next_inner_pid, next_inner, NULL)) {
        break;
      }
    }

    // LOG_INFO(" new pid = %ld", next_inner_pid);
    for (unsigned short slot = pos + 1; slot < num_key; slot++) {
      // LOG_INFO(" slot = %hu, pid = %ld", slot, buffer[slot].second);
      next_inner->SetSlot(slot - pos - 1, buffer[slot].first,
                          buffer[slot].second);
    }

    SplitNode *split_delta =
        AllocateSplit(split_key, next_inner_pid, n->GetLevel());

    split_delta->SetBase(n);
    if (n->IsDelta()) {
      split_delta->SetLength(static_cast<DeltaNode *>(n)->GetLength() + 1);
    } else {
      split_delta->SetLength(1);
    }
    split_delta->SetSize(num_key / 2 - 1);

    if (mapping_table.Update(pid, split_delta, n)) {
      base_node->SetNext(next_inner_pid);
      next_inner->SetNext(former_next_inner_pid);

      for (unsigned short slot = 0; slot <= next_inner->GetSize(); slot++) {
        GetBaseNode(GetNode(next_inner->child_pid[slot]))
            ->SetParent(next_inner_pid);
      }

      // LOG_INFO("split delta succeeded");
      // LOG_INFO("left  size = %ld", split_delta->GetSize());
      // LOG_INFO("right size = %ld", next_inner->GetSize());
      break;
    } else {
      FreeNode(next_inner);
      FreeNode(split_delta);
      // LOG_INFO("split delta failed");
    }
  }

  // LOG_INFO("split delta is added");

  // separator delta node
  for (;;) {
    Node *parent = GetNode(parent_pid);
    KeyType right_key = FindUpperKey(parent_pid, split_key);

    SeparatorNode *separator_delta = AllocateSeparator(
        split_key, right_key, next_inner_pid, parent->GetLevel());
    separator_delta->SetBase(parent);
    if (parent->IsDelta()) {
      separator_delta->SetLength(static_cast<DeltaNode *>(parent)->GetLength() +
                                 1);
    } else {
      separator_delta->SetLength(1);
    }
    separator_delta->SetSize(1 + parent->GetSize());

    if (mapping_table.Update(parent_pid, separator_delta, parent)) {
      if (separator_delta->IsInnerFull()) {
        SplitInner(parent_pid);
      }
      break;
    } else {
      separator_delta->child = NULL_PID;
      FreeNode(separator_delta);
    }
  }

  // LOG_INFO("index entry is added");

  // LOG_INFO("split inner is done");
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Exists(
    const KeyType &key) {
  PID leaf_pid = GetLeafNodePID(key);

  if (leaf_pid < 0) {
    return false;
  }

  Node *leaf = mapping_table.Get(leaf_pid);

  // Traversing delta chain here will be more efficient since we dont have to
  // loop till base for some cases.
  // But for simplicity, we call get_all_data() and check if the key is in the
  // returned vector.
  std::vector<DataPairListType> node_data = GetAllData(leaf);

  return VectorContainsKey2(node_data, key);
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
std::vector<std::pair<KeyType, ValueType>>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Search(
    const KeyType &key) {
  std::vector<DataPairType> result;

  PID leaf_pid = GetLeafNodePID(key);
  // LOG_INFO("search is called");
  if (leaf_pid < 0) {
    // LOG_INFO("no node found");
    return result;
  }

  // Find the leaf node and retrieve all records in the node
  Node *leaf = GetNode(leaf_pid);

  //  if (leaf->IsDelta()) {
  //    DeltaNode *delta = static_cast<DeltaNode *>(leaf);
  //    if (delta->GetLength() > DELTA_THRESHOLD) {
  //      ConsolidateLeafNode(leaf_pid);
  //    }
  //  }

  auto node_data = GetAllData(leaf);
  // LOG_INFO("get_all_data is done on %ld, length = %ld", leaf_pid,
  // node_data.size());

  // Check if we have a match (possible improvement: implement binary search)
  for (auto it = node_data.begin(); it != node_data.end(); ++it) {
    // // LOG_INFO(".");
    // // LOG_INFO(" size = %d", it->second.GetSize());
    // For duplicate keys, there's an edge case that some records can be placed
    // at the
    // next node so we need to check the next page as well.
    //    if(it != node_data.end() && (next(it) == node_data.end()) &&
    //    key_equal(key, it->first)) {
    //      // TODO: handle duplicate keys
    //    }
    if (KeyEqual(key, it->first)) {
      // LOG_INFO("key match found");
      // // LOG_INFO(" size = %d", it->second.GetSize());
      for (int i = 0; i < it->second.GetSize(); i++) {
        result.push_back(std::make_pair(it->first, it->second.GetValue(i)));
      }
    }
  }
  // LOG_INFO("search is done");
  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
std::vector<std::pair<KeyType, ValueType>>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::SearchAll() {
  std::vector<DataPairType> result;
  // LOG_INFO("search all is called");

  // Find the leaf node and retrieve all records in the node
  PID leaf_pid = m_headleaf;
  Node *leaf = mapping_table.Get(leaf_pid);
  while (leaf_pid != NULL_PID) {
    // LOG_INFO("search for leaf pid = %ld", leaf_pid);
    if (leaf == NULL) {
      // LOG_INFO("search for leaf pid = %ld, node is null", leaf_pid);
    }
    if (leaf->IsDelta()) {
      // LOG_INFO("length = %ld", static_cast<DeltaNode *>(leaf)->GetLength());
    }
    // LOG_INFO("node size = %ld", leaf->GetSize());
    auto node_data = GetAllData(leaf);

    // LOG_INFO("search done for leaf pid = %ld, size = %ld", leaf_pid,
    // node_data.size());
    // Check if we have a match (possible improvement: implement binary search)
    for (auto it = node_data.begin(); it != node_data.end(); ++it) {
      // LOG_INFO(" key length = %d", it->second.GetSize());
      for (int i = 0; i < it->second.GetSize(); i++) {
        result.push_back(std::make_pair(it->first, it->second.GetValue(i)));
      }
      // result.push_back(*it);
    }

    // LOG_INFO("result length = %ld", result.size());

    leaf_pid = static_cast<LeafNode *>(GetBaseNode(leaf))->GetNext();
    if (leaf_pid != NULL_PID) {
      leaf = GetNode(leaf_pid);
    }
  }
  // LOG_INFO("search all is done");
  return result;
}

// template <typename KeyType, typename ValueType, typename KeyComparator,
// typename KeyEqualityChecker>
// std::vector<std::pair<KeyType, ValueType>> BWTree<KeyType, ValueType,
// KeyComparator, KeyEqualityChecker>::SearchRange(const KeyType &low, const
// KeyType &high) {
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
//  // const leaf_node* low_node = static_cast<const
//  leaf_node*>(mapping_table.get(low_pid));
//  // const leaf_node* high_node = static_cast<const
//  leaf_node*>(mapping_table.get(high_pid));
//  // PID current_pid = low_pid;
//
//  return result;
//
//}

// template <typename KeyType, typename ValueType, typename KeyComparator,
// typename KeyEqualityChecker>
// void BWTree<KeyType, ValueType, KeyComparator,
// KeyEqualityChecker>::ConsolidateInnerNode(
//  PID pid) {
//
//  // This node must be delta node since we are calling consolidation when the
//  threshold for delta length exceeds
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

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator,
            KeyEqualityChecker>::ConsolidateLeafNode(PID pid) {
  for (;;) {
    LOG_INFO("CONSOLIDATION STARTS!");

    // This node must be delta node since we are calling consolidation when the
    // threshold for delta length exceeds
    Node *old = mapping_table.Get(pid);

    Node *node = old;
    LeafNode *consolidated = AllocateLeaf();

    consolidated->next_leaf = NULL_PID;
    consolidated->prev_leaf = NULL_PID;

    // Set parent, level information
    consolidated->parent = node->parent;
    consolidated->level = node->level;

    // Get the base node
    if (node->IsDelta()) {
      node = static_cast<DeltaNode *>(node)->GetBase();
    }

    consolidated->next_leaf = static_cast<LeafNode *>(node)->next_leaf;
    consolidated->prev_leaf = static_cast<LeafNode *>(node)->prev_leaf;

    // Set the key slot, data.
    auto data = GetAllData(node);
    consolidated->slot_use = data.size();

    int index = 0;
    for (auto it = data.begin(); it != data.end(); ++it) {
      consolidated->slot_key[index] = it->first;
      consolidated->slot_data[index] = it->second;
      index++;
    }

    LOG_INFO("consolidated node next_leaf(%ld), prev_leaf(%ld), parent(%ld)",
             consolidated->next_leaf, consolidated->prev_leaf,
             consolidated->parent);

    // Check if there was any change in the mapping table while consolidating
    if (mapping_table.Update(pid, consolidated, old)) {
      epoch_table.RegisterNode(old);
      break;
    } else {
      FreeNode(consolidated);
    }
  }
}

// Debug Purpose
template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Print() {
  // LOG_INFO("bw tree print");
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
