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
#include <vector>
#include <atomic>
#include <iostream>

#define BWTREE_MAX(a,b) ((a) < (b) ? (b) : (a))
#define BWTREE_NODE_SIZE 256
#define MAPPING_TABLE_SIZE 4096

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
    update_node,
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

  // ValueList struct for handling duplicate keys
  struct ValueList {
    std::vector<ValueType> value_list;

    // Insert Value to the value list. If the value already exists, we can ignore
    inline void InsertValue(ValueType value) {
      // if(FindValue(value) != -1) {
      //   return;
      // } else {
        value_list.emplace_back(value);
      // }
    }


    // Return the value at the given index
    inline ValueType GetValue(int index) {
      return value_list.at(index);
    }

    // Remove the value
    inline void RemoveValue(ValueType value) {
      int index = FindValue(value);
      if(index == -1) {
        return;
      } else {
        value_list.erase(value_list.begin() + index);
      }
    }

    // Find the index of value ( -1 indicates not exist)
    inline int FindValue(ValueType value) {
      int index = 0;
      for (auto it = value_list.begin() ; it != value_list.end(); ++it) {
        if(it->block == value.block && it->offset == value.offset) {
          return index;
        }
        index ++;
      }
      return -1;
    }

    inline int GetSize() {
      return value_list.size();
    }

//    ~ValueList() {
//      delete value_list;
//    }
  };


  typedef std::pair<KeyType, ValueList> DataPairListType;

  // *** Node Classes for In-Memory Nodes

  /// The header structure of each node in memory. This structure is extended
  /// by inner_node or leaf_node or delta_node.
  struct Node {
    NodeType node_type;
    unsigned short level;
    unsigned short slot_use;

    PID parent;

    inline void Initialize(NodeType n, unsigned short l, unsigned short s) {
      node_type = n;
      level = l;
      slot_use = s;
      parent = NULL_PID;
    }

    inline bool IsLeaf() const {
      return (level == 0);
    }

    inline bool IsDelta() const {
      return (node_type != NodeType::leaf_node && node_type != NodeType::inner_node);
    }

    inline bool IsFull() const {
      return (slot_use == leaf_slot_max);
    }

    inline bool IsFew() const {
      return (slot_use <= min_leaf_slots);
    }

    inline bool IsUnderflow() const {
      return (slot_use < min_leaf_slots);
    }

    inline NodeType GetType() const {
      return node_type;
    }

    inline unsigned short GetLevel() const {
      return level;
    }

    inline size_t GetSize() const {
      return slot_use;
    }

    inline void SetSize(unsigned short size) {
      slot_use = size;
    }

    inline void AddSlotUse() {
      slot_use++;
    }

    inline PID GetParent() const {
      return parent;
    }

    inline void SetParent(PID p) {
      parent = p;
    }

  };

  /// Extended structure of a inner node in memory. Contains only keys and no
  /// data items.
  struct InnerNode : public Node {

    typedef typename AllocType::template rebind<InnerNode>::other alloc_type;

    KeyType slot_key[inner_slot_max];

    PID child_pid[inner_slot_max + 1];

    inline void Initialize(unsigned short l, PID child) {
      child_pid[0] = child;
      Node::Initialize(NodeType::inner_node, l, 0);
    }

    inline void SetSlot(unsigned short slot, KeyType k, PID p) {
      if (slot >= Node::GetSize())
          Node::AddSlotUse();
      slot_key[slot] = k;
      child_pid[slot + 1] = p;
    }
  };

  /// Extended structure of a leaf node in memory. Contains pairs of keys and
  /// data items.
  struct LeafNode : public Node {

    typedef typename AllocType::template rebind<LeafNode>::other alloc_type;

    PID prev_leaf;

    PID next_leaf;

    KeyType slot_key[leaf_slot_max];

    ValueList slot_data[leaf_slot_max];

    inline void Initialize() {
      Node::Initialize(NodeType::leaf_node, 0, 0);
      prev_leaf = next_leaf = NULL_PID;
    }

    inline void SetSlot(unsigned short slot, const DataPairListType &pair) {
      if (slot >= Node::GetSize())
          Node::AddSlotUse();
      slot_key[slot] = pair.first;
      slot_data[slot] = pair.second;
    }

    inline PID GetPrev() const {
      return prev_leaf;
    }

    inline void SetPrev(PID pid) {
      prev_leaf = pid;
    }

    inline PID GetNext() const {
      return next_leaf;
    }

    inline void SetNext(PID pid) {
      next_leaf = pid;
    }

  };

  /// Extended structure of a delta node in memory. Contains a physical
  /// pointer to base page.
  struct DeltaNode : public Node {
    Node *base;
    size_t chain_length;

    inline void Initialize(NodeType t, size_t l) {
      chain_length = 0;
      // if (base->IsDelta()) {
      //   chain_length = static_cast<DeltaNode *>(base)->GetLength() + 1;
      // }
      Node::Initialize(t, l, 0);
    }

    inline void SetBase(Node *n) {
      base = n;
    }

    inline Node *GetBase() const {
      return base;
    }

    inline size_t GetLength() const {
      return chain_length;
    }

    inline void SetLength(size_t l) {
      chain_length = l;
    }
  };

  /// Extended structure of a delta node in memory. Contains a key, value
  /// pair to insert
  struct InsertNode : public DeltaNode {
    typedef typename AllocType::template rebind<InsertNode>::other alloc_type;

    KeyType insert_key;
    ValueType insert_value;

    inline void Initialize(const DataPairType &pair, size_t l) {
      insert_key = pair.first;
      insert_value = pair.second;
      DeltaNode::Initialize(NodeType::insert_node, l);
    }

    inline DataPairType GetData() const {
      return std::make_pair(insert_key, insert_value);
    }
  };

  /// Extended structure of a delta node in memory. Contains a key to delete
  struct DeleteNode : public DeltaNode {
    typedef typename AllocType::template rebind<DeleteNode>::other alloc_type;

    KeyType delete_key;
    bool has_value;
    ValueType delete_value;

    inline void InitializeNoValue(const KeyType &key, size_t l) {
      delete_key = key;
      has_value = false;
      DeltaNode::Initialize(NodeType::delete_node, l);
    }

    inline void InitializeWithValue(const DataPairType &pair, size_t l) {
      delete_key = pair.first;
      delete_value = pair.second;
      has_value = true;
      DeltaNode::Initialize(NodeType::delete_node, l);
    }

    inline KeyType GetKey() const {
      return delete_key;
    }

    inline DataPairType GetData() const {
      return std::make_pair(delete_key, delete_value);
    }
  };

  /// Extended structure of a update node in memory. Contains a key, value
  /// pair to update
  struct UpdateNode : public DeltaNode {
    typedef typename AllocType::template rebind<UpdateNode>::other alloc_type;

    KeyType update_key;
    ValueType update_value;

    inline void Initialize(const DataPairType &pair, size_t l) {
      update_key = pair.first;
      update_value = pair.second;
      DeltaNode::Initialize(NodeType::update_node, l);
    }

    inline DataPairType get_data() const {
      return std::make_pair(update_key, update_value);
    }
  };

  /// Extended structure of a delta node in memory. Contains a split key
  /// and a logical side pointer.
  struct SplitNode : public DeltaNode {
    typedef typename AllocType::template rebind<SplitNode>::other alloc_type;

    KeyType split_key;
    PID side;

    inline void Initialize(KeyType &key, PID pid, size_t l) {
      split_key = key;
      side = pid;
      DeltaNode::Initialize(NodeType::split_node, l);
    }

    inline KeyType GetKey() const {
      return split_key;
    }

    inline PID GetSide() const {
      return side;
    }
  };

  /// Extended structure of a delta node in memory. Contains a key range
  /// [min_key, max_key) and a logical pointer to the child.
  struct SeparatorNode : public DeltaNode {
    typedef typename AllocType::template rebind<SeparatorNode>::other alloc_type;

    KeyType left;
    KeyType right;
    PID child;

    bool right_most;

    inline void Initialize(const KeyType &left_key, const KeyType &right_key, const PID pid, bool r, size_t l) {
      left = left_key;
      right = right_key;
      child = pid;
      right_most = r;
      DeltaNode::Initialize(NodeType::separator_node, l);
    }
  };

  struct MappingTable {

    Node** table = new Node*[MAPPING_TABLE_SIZE]();

    inline void Initialize() {
      std::fill_n(table, MAPPING_TABLE_SIZE, 0);
    }

    // Atomically update the value using CAS
    inline bool Update(PID key, Node *value, Node *old, size_t delta) {
      // Node *head = table[key];
      if (__sync_bool_compare_and_swap(&table[key], old, value) == true) {
        if (old != NULL) {
          static_cast<DeltaNode *>(value)->SetBase(old);
          if (old->IsDelta()) {
            static_cast<DeltaNode *>(value)->SetLength(static_cast<DeltaNode *>(old)->GetLength() + 1);
          } else {
            static_cast<DeltaNode *>(value)->SetLength(1);
          }
          if (delta > 1) {
            value->SetSize(delta);
          } else {
            value->SetSize(delta + old->GetSize());
          }
        } else {
          value->SetSize(delta);
        }
        return true;  // Update success
      }
      return false;
    }

    // Mark as null if remove is called
    inline bool Remove(PID key) {
      if (__sync_bool_compare_and_swap(&table[key], table[key], NULL) == true) {
        return true;
      }
      return false;
    }

    inline int GetSize() {
      return MAPPING_TABLE_SIZE;
    }

    // Get physical pointer from PID
    inline Node* Get(PID key) {
      return table[key];
    }

    // This will be changed if we will not use array
    inline bool ContainsKey(PID key) {
      if(table[key]){
        return true;
      }
      return false;
    }

    ~MappingTable(){
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
  MappingTable mapping_table;

  /// Key comparator
  KeyComparator m_comparator;

  /// Atomic counter for PID allocation
  std::atomic<int> pid_counter;

public:

  /// Default constructor initializing an empty BW tree with the standard key
  /// comparison function
  explicit inline BWTree(const KeyComparator &kcf,
                         const AllocType &alloc = AllocType())
      : m_root(NULL_PID), m_headleaf(NULL_PID), m_tailleaf(NULL_PID), m_allocator(alloc), m_comparator(kcf), pid_counter(0) {
    mapping_table.Initialize();
  }


  /// Frees up all used B+ tree memory pages
  inline ~BWTree() {
    Clear();
  }

private:
  // *** Convenient Key Comparison Functions Generated From key_less


  /// True if a < b ? "constructed" from m_key_less()
  inline bool KeyLess(const KeyType &a, const KeyType &b) const {
    return m_comparator(a, b);
  }

  /// True if a < b ? "constructed" from m_key_less()
  inline bool DatapairLess(const DataPairType &a, const DataPairType &b) const {
    return m_comparator(a.first, b.first);
  }

  // struct {
  //   KeyComparator cmp;
  //   bool operator()(const DataPairType &a, const DataPairType &b) {   
  //     return cmp(a.first, b.first);
  //   }   
  // } data_comparator;

  /// True if a <= b ? constructed from key_less()
  inline bool KeyLessEqual(const KeyType &a, const KeyType &b) const {
    return !m_comparator(b, a);
  }

  /// True if a > b ? constructed from key_less()
  inline bool KeyGreater(const KeyType &a, const KeyType &b) const {
    return m_comparator(b, a);
  }

  /// True if a >= b ? constructed from key_less()
  inline bool KeyGreaterEqual(const KeyType &a, const KeyType &b) const {
    return !m_comparator(a, b);
  }

  /// True if a == b ? constructed from key_less(). This requires the <
  /// relation to be a total order, otherwise the B+ tree cannot be sorted.
  inline bool KeyEqual(const KeyType &a, const KeyType &b) const {
    return !m_comparator(a, b) && !m_comparator(b, a);
  }


private:
  // *** Node Object Allocation and Deallocation Functions

  typename LeafNode::alloc_type LeafNodeAllocator() {
    return typename LeafNode::alloc_type(m_allocator);
  }

  /// Return an allocator for inner_node objects
  typename InnerNode::alloc_type InnerNodeAllocator() {
    return typename InnerNode::alloc_type(m_allocator);
  }

  /// Return an allocator for insert_node objects
  typename InsertNode::alloc_type InsertNodeAllocator() {
    return typename InsertNode::alloc_type(m_allocator);
  }

  /// Return an allocator for delete_node objects
  typename DeleteNode::alloc_type DeleteNodeAllocator() {
    return typename DeleteNode::alloc_type(m_allocator);
  }

  /// Return an allocator for update_node objects
  typename UpdateNode::alloc_type UpdateNodeAllocator() {
    return typename UpdateNode::alloc_type(m_allocator);
  }

  /// Return an allocator for split_node objects
  typename SplitNode::alloc_type SplitNodeAllocator() {
    return typename SplitNode::alloc_type(m_allocator);
  }

  /// Return an allocator for separator_node objects
  typename SeparatorNode::alloc_type SeparateNodeAllocator() {
    return typename SeparatorNode::alloc_type(m_allocator);
  }

  /// Allocate and initialize a leaf node
  inline LeafNode *AllocateLeaf() {
    LeafNode *n = new (LeafNodeAllocator().allocate(1)) LeafNode();
    n->Initialize();
    // PID pid = AllocatePID();
    // mapping_table.Update(pid, n);
    return n;
  }

  /// Allocate and initialize an inner node
  inline InnerNode *AllocateInner(unsigned short level, PID child) {
    InnerNode *n = new (InnerNodeAllocator().allocate(1)) InnerNode();
    n->Initialize(level, child);
    // PID pid = AllocatePID();
    // mapping_table.Update(pid, n);
    return n;
  }

  /// Allocate and initialize an insert delta node
  inline InsertNode *AllocateInsert(const DataPairType &pair, size_t l) {
    InsertNode *n = new (InsertNodeAllocator().allocate(1)) InsertNode();
    n->Initialize(pair, l);
    return n;
  }

  /// Allocate and initialize an delete delta node
  inline DeleteNode *AllocateDeleteNoValue(const KeyType &key, size_t l) {
    DeleteNode *n = new (DeleteNodeAllocator().allocate(1)) DeleteNode();
    n->InitializeNoValue(key, l);
    return n;
  }

  /// Allocate and initialize an delete delta node
  inline DeleteNode *AllocateDeleteWithValue(const DataPairType &key, size_t l) {
    DeleteNode *n = new (DeleteNodeAllocator().allocate(1)) DeleteNode();
    n->InitializeWithValue(key, l);
    return n;
  }

  /// Allocate and initialize an insert delta node
  inline UpdateNode *AllocateUpdate(const DataPairType &pair, size_t l) {
    UpdateNode *n = new (UpdateNodeAllocator().allocate(1)) UpdateNode();
    n->Initialize(pair, l);
    return n;
  }

  /// Allocate and initialize an split delta node
  inline SplitNode *AllocateSplit(KeyType &key, PID leaf, size_t l) {
    SplitNode *n = new (SplitNodeAllocator().allocate(1)) SplitNode();
    n->Initialize(key, leaf, l);
    return n;
  }

  /// Allocate and initialize an separator delta node
  inline SeparatorNode *AllocateSeparator(KeyType &left_key, KeyType &right_key, PID leaf, size_t l) {
    SeparatorNode *n = new (SeparateNodeAllocator().allocate(1)) SeparatorNode();
    if (KeyEqual(left_key, right_key)) {
      n->Initialize(left_key, right_key, leaf, true, l);
    } else {
      n->Initialize(left_key, right_key, leaf, false, l);
    }
    return n;
  }

  /// Correctly free either inner or leaf node, destructs all contained key
  /// and value objects & frees delta nodes
  inline void FreeNode(Node *n)
  {
    switch (n->GetType()) {
      case NodeType::leaf_node:
        {
          LeafNode *ln = static_cast<LeafNode *>(n);
          typename LeafNode::alloc_type a(LeafNodeAllocator());
          a.destroy(ln);
          a.deallocate(ln, 1);
        }
        break;
      case NodeType::inner_node: {
        InnerNode *inner = static_cast<InnerNode *>(n);
        typename InnerNode::alloc_type a(InnerNodeAllocator());
        a.destroy(inner);
        a.deallocate(inner, 1);
      }
        break;
      case NodeType::insert_node: {
        InsertNode *ins = static_cast<InsertNode *>(n);
        typename InsertNode::alloc_type a(InsertNodeAllocator());
        a.destroy(ins);
        a.deallocate(ins, 1);
      }
        break;
      case NodeType::delete_node: {
        DeleteNode *del = static_cast<DeleteNode *>(n);
        typename DeleteNode::alloc_type a(DeleteNodeAllocator());
        a.destroy(del);
        a.deallocate(del, 1);
      }
        break;
      case NodeType::update_node:
        break;
      case NodeType::split_node: {
        SplitNode *split = static_cast<SplitNode *>(n);
        typename SplitNode::alloc_type a(DeleteNodeAllocator());
        // if(mapping_table.ContainsKey(split->side)) {
        //   ClearRecursive(split->side);
        // }
        a.destroy(split);
        a.deallocate(split, 1);
      }
        break;
      case NodeType::separator_node: {
        SeparatorNode *sep = static_cast<SeparatorNode *>(n);
        typename SeparatorNode::alloc_type a(DeleteNodeAllocator());
        if(mapping_table.ContainsKey(sep->child)) {
          ClearRecursive(sep->child);
        }
        a.destroy(sep);
        a.deallocate(sep, 1);
      }
        break;
    }
  }

public:
  void Clear() {
    if(m_root != NULL_PID) {
      ClearRecursive(m_root);
    }
  }


private:

  void ClearRecursive(PID pid) {
    if(!mapping_table.ContainsKey(pid)) {
      return;
    }
    Node* node = mapping_table.Get(pid);
    while(node->IsDelta()) {
      Node* prev= node;
      node = static_cast<DeltaNode *>(node)->GetBase();
      FreeNode(prev);
    }

    if(node->GetType() == NodeType::leaf_node) {
      LeafNode* leaf_node = static_cast<LeafNode*>(node);
      FreeNode(leaf_node);

    } else if (node->GetType() == NodeType::inner_node) {
      InnerNode* inner_node = static_cast<InnerNode *>(node);
      for (unsigned short slot = 0; slot < inner_node->slot_use + 1; ++slot)
      {
        ClearRecursive(inner_node->child_pid[slot]);
      }
      FreeNode(inner_node);
    }
    mapping_table.Remove(pid);
  }

  inline PID AllocatePID() {
    pid_counter++;
    return pid_counter;
  }

  inline Node *GetNode(PID pid) {
    if (mapping_table.Get(pid) == NULL)
      return NULL;
    return mapping_table.Get(pid);
  }

  // inline void SetNode(PID pid, Node *n) {
  //   mapping_table.Update(pid, n);
  // }

private:
  inline unsigned short FindLower(const InnerNode *n, const KeyType &key) const {
    unsigned short lo = 0;
    while (lo < n->slot_use && KeyLess(n->slot_key[lo], key)) ++lo;
    return lo;
  }

  inline KeyType FindUpperKey(PID pid, const KeyType &key) {
    Node *node = mapping_table.Get(pid);
    KeyType upper_key = key;

    while (node->IsDelta()) {
      switch (node->GetType()) {
        case NodeType::leaf_node:
          break;
        case NodeType::inner_node:
          break;
        case NodeType::insert_node:
          break;
        case NodeType::delete_node:
          break;
        case NodeType::update_node:
          break;
        case NodeType::split_node:
          break;
        case NodeType::separator_node:
          KeyType left = static_cast<SeparatorNode *>(node)->left;
          if (KeyLess(key, left) && (KeyEqual(key, upper_key) || KeyLess(left, upper_key))) {
            upper_key = left;
          }
          break;
      }
      node = static_cast<DeltaNode *>(node)->GetBase();
    }
    InnerNode *inner = static_cast<InnerNode *>(node);
    for (unsigned short lo = 0; lo < inner->slot_use; lo++) {
      if (KeyLess(key, inner->slot_key[lo])) {
        if (KeyEqual(key, upper_key) || KeyLess(inner->slot_key[lo], upper_key)) {
          upper_key = inner->slot_key[lo];
        }
        break;
      }
    }
    return upper_key;
  }

  inline PID FindNextPID(PID pid, const KeyType &key) {
    Node *node = mapping_table.Get(pid);
    while (node->IsDelta()) {
      switch (node->GetType()) {
        case NodeType::leaf_node:
          break;
        case NodeType::inner_node:
          break;
        case NodeType::insert_node:
          break;
        case NodeType::delete_node:
          break;
        case NodeType::update_node:
          break;
        case NodeType::split_node:
          break;
        case NodeType::separator_node:
          KeyType left = static_cast<SeparatorNode *>(node)->left;
          KeyType right = static_cast<SeparatorNode *>(node)->right;
          bool right_most = static_cast<SeparatorNode *>(node)->right_most;
          if (KeyLessEqual(left, key) && (right_most || KeyLess(key, right))) {
            return static_cast<SeparatorNode *>(node)->child;
          }
          break;
      }
      node = static_cast<DeltaNode *>(node)->GetBase();
    }
    InnerNode *inner = static_cast<InnerNode *>(node);
    unsigned short lo = 0;
    while (lo < inner->slot_use && KeyLess(inner->slot_key[lo], key)) ++lo;
    return inner->child_pid[lo];
  }

  inline bool LeafContainsKey(Node *node, const KeyType &key) {
    while (node->IsDelta()) {
      switch (node->GetType()) {
        case NodeType::leaf_node:
          break;
        case NodeType::inner_node:
          break;
        case NodeType::insert_node:
          if (KeyEqual(key, static_cast<InsertNode *>(node)->insert_key)) {
            return true;
          }
          break;
        case NodeType::delete_node:
          if (KeyEqual(key, static_cast<DeleteNode *>(node)->GetKey())) {
            return false;
          }
          break;
        case NodeType::update_node:
          break;
        case NodeType::split_node:
          break;
        case NodeType::separator_node:
          break;
      }
      node = static_cast<DeltaNode *>(node)->GetBase();
    }
    LeafNode *leaf = static_cast<LeafNode *>(node);
    for (unsigned short slot = 0; slot < leaf->GetSize(); slot++) {
      if (KeyEqual(key, leaf->slot_key[slot])) {
        return true;
      }
    }
    return false;
  }

  inline Node *GetBaseNode(Node *n) const {
    while (n->IsDelta()) {
      n = static_cast<DeltaNode *>(n)->GetBase();
    }
    return n;
  }

  inline std::vector<DataPairListType> GetAllData(Node *n) {
    std::vector<DataPairType> inserted;
    std::vector<DataPairType> deleted;
    std::vector<KeyType> deleted_key;
    bool has_split = false;
    KeyType split_key;

    DataPairType data;

    while (n->IsDelta()) {
      switch (n->GetType()) {
        case NodeType::insert_node:
          data = static_cast<InsertNode *>(n)->GetData();
          if ((!has_split || KeyLess(data.first, split_key))
              && !VectorContainsData(deleted, data)
              && !KeyVectorContainsKey(deleted_key, data.first)) {
            inserted.push_back(data);
          }
          break;

        case NodeType::delete_node:
          if (static_cast<DeleteNode *>(n)->has_value) {
            deleted.push_back(static_cast<DeleteNode *>(n)->GetData());
          } else {
            deleted_key.push_back(static_cast<DeleteNode *>(n)->GetKey());
          }
          break;

        case NodeType::update_node:
          data = static_cast<UpdateNode *>(n)->get_data();
          if ((!has_split || KeyLess(data.first, split_key))
              && !VectorContainsData(deleted, data)
              && !KeyVectorContainsKey(deleted_key, data.first)) {
            inserted.push_back(data);
          }
          break;

        case NodeType::split_node:
          if (!has_split) {
            split_key = static_cast<SplitNode *>(n)->GetKey();
            has_split = true;
          }
          break;
        case NodeType::leaf_node:
          break;
        case NodeType::inner_node:
          break;
        case NodeType::separator_node:
          break;
      }
      n = static_cast<DeltaNode *>(n)->GetBase();
    }
    std::vector<DataPairListType> result;


    LeafNode *leaf = static_cast<LeafNode *>(n);
    for (unsigned short slot = 0; slot < leaf->GetSize(); slot++) {
      if ((!has_split || KeyLess(leaf->slot_key[slot], split_key))
          && !KeyVectorContainsKey(deleted_key, leaf->slot_key[slot])) {
        result.push_back(std::make_pair(leaf->slot_key[slot], leaf->slot_data[slot]));
      }
    }

    for (int i = 0; i < deleted.size(); i++) {
      for (int j = 0; j < result.size(); j++) {
        if (KeyEqual(deleted[i].first, result[j].first)) {
          result[j].second.RemoveValue(deleted[i].second);
        }
      }
    }


    for (int i = 0; i < inserted.size(); i++) {
      bool match = false;
      for (int j = 0; j < result.size(); j++) {
        if (KeyEqual(inserted[i].first, result[j].first)) {
          match = true;
          result[j].second.InsertValue(inserted[i].second);
          break;
        }
      }
      if (!match) {
        ValueList value_list;
        value_list.InsertValue(inserted[i].second);
        result.push_back(std::make_pair(inserted[i].first, value_list));
      }
    }
    if (result.size() == 0) {
      return result;
    }

    // std::sort(result.begin(), result.end(), data_comparator);
    for (int i = 0; i < result.size() - 1; i++)
      for (int j = i + 1; j < result.size(); j++) {
        if (KeyGreater(result[i].first, result[j].first)) {
          DataPairListType tmp = result[i];
          result[i] = result[j];
          result[j] = tmp;
        }
      }
    return result;
  }

  // Helper function for checking if the key is in the vector.
  inline bool VectorContainsKey(std::vector<DataPairType> & data, const KeyType &key) const {
    for (auto it = data.begin() ; it != data.end(); ++it) {
      if(KeyEqual(key, it->first)) {
        return true;
      }
    }
    return false;
  }


  // Helper function for checking if the key is in the vector.
  inline bool VectorContainsKey2(std::vector<DataPairListType> & data, const KeyType &key) const {
    for (auto it = data.begin() ; it != data.end(); ++it) {
      if(KeyEqual(key, it->first)) {
        return true;
      }
    }
    return false;
  }


  // Helper function for checking if the data is in the vector.
  inline bool VectorContainsData(std::vector<DataPairType> & data, const DataPairType &pair) const {
    KeyType key = pair.first;
    ValueType value = pair.second;
    for (auto it = data.begin() ; it != data.end(); ++it) {
      if (KeyEqual(key, it->first)
        && value.block == (it->second).block
        && value.offset == (it->second).offset) {

        return true;
      }
    }
    return false;
  }

  // Helper function for checking if the key is in the vector.
  inline bool KeyVectorContainsKey(std::vector<KeyType> & keys, const KeyType &key) const {
    for (auto it = keys.begin() ; it != keys.end(); ++it) {
      if(KeyEqual(key, *it)) {
        return true;
      }
    }
    return false;
  }

  // Returns the pid of the page that contains the target key
  // Currently, returns -1 for error
  inline PID GetLeafNodePID(const KeyType &key) {
    PID current_pid = m_root;
    Node* current = mapping_table.Get(m_root);

    if(!current) return -1;

    // Keep traversing tree until we find the target leaf node
    while(!current->IsLeaf()) {

      current_pid = FindNextPID(current_pid, key);
      current = mapping_table.Get(current_pid);

      // NodeType current_type = current->GetType();
      // // We need to take care of delta nodes from split/merge and regular inner node
      // switch(current_type) {

      //   case NodeType::insert_node:
      //     break;
      //   case NodeType::delete_node:
      //     break;
      //   case NodeType::update_node:
      //     break;
      //   case NodeType::split_node:
      //     // if we are at the split node, check separator key K_p
      //     // if key > K_p, we go to the side node in order to look up keys
      //     break;
      //   case NodeType::separator_node:
      //     break;

      //   case NodeType::leaf_node:
      //     break;
      //   case NodeType::inner_node :
      //     // const InnerNode* current_inner = static_cast<const InnerNode *>(current);
      //     // int slot = FindLower(current_inner, key);
      //     // current_pid = current_inner->child_pid[slot];
      //     current_pid = FindNextPID(current_pid, key);
      //     current = mapping_table.Get(current_pid);
      //     break;
      // }
    }

    return current_pid;
  }

private:
  size_t Count(const KeyType &key);
  // Split/ Merge are internal
  void SplitLeaf(PID pid);

  void ConsolidateNode(PID pid);

public:
  // BW Tree API
  void InsertData(const DataPairType &x);
  void DeleteKey(const KeyType &x);
  void DeleteData(const DataPairType &x);
  void UpdateData(const DataPairType &x);
  bool Exists(const KeyType &key);
  std::vector<DataPairType> Search(const KeyType &key);
  std::vector<DataPairType> SearchAll();
  std::vector<DataPairType> Scan();


public:
  // *** Debug Printing

  // Print out the BW tree structure.
  void Print();

private:

  // Print out the BW tree node
  void PrintNode(const Node* node);
};

}  // End index namespace
}  // End peloton namespace
