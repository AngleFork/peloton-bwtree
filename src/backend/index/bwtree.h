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

    inline void Initialize(unsigned short l) {
      Node::Initialize(NodeType::inner_node, l, 0);
    }

    inline void SetSlot(unsigned short slot, KeyType k, PID p) {
      if (slot >= Node::GetSize())
          Node::AddSlotUse();
      slot_key[slot] = k;
      child_pid[slot] = p;
    }
  };

  /// Extended structure of a leaf node in memory. Contains pairs of keys and
  /// data items.
  struct LeafNode : public Node {

    typedef typename AllocType::template rebind<LeafNode>::other alloc_type;

    PID prev_leaf;

    PID next_leaf;

    KeyType slot_key[leaf_slot_max];

    ValueType slot_data[leaf_slot_max];

    inline void Initialize() {
      Node::Initialize(NodeType::leaf_node, 0, 0);
      prev_leaf = next_leaf = NULL_PID;
    }

    inline void SetSlot(unsigned short slot, const DataPairType &pair) {
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

    inline void Initialize(NodeType t, unsigned short s, Node *n) {
      base = n;
      chain_length = 0;
      if (base->IsDelta()) {
        chain_length = static_cast<DeltaNode *>(base)->GetLength() + 1;
      }
      Node::Initialize(t, base->GetLevel(), s);
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
  };

  /// Extended structure of a delta node in memory. Contains a key, value
  /// pair to insert
  struct InsertNode : public DeltaNode {
    typedef typename AllocType::template rebind<InsertNode>::other alloc_type;

    KeyType insert_key;
    ValueType insert_value;

    inline void Initialize(const DataPairType &pair, Node *n) {
      insert_key = pair.first;
      insert_value = pair.second;
      DeltaNode::Initialize(NodeType::insert_node, n->GetSize() + 1, n);
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

    inline void InitializeNoValue(const KeyType &key, Node *n) {
      delete_key = key;
      has_value = false;
      DeltaNode::Initialize(NodeType::delete_node, n->GetSize() - 1, n);
    }

    inline void InitializeWithValue(const DataPairType &pair, Node *n) {
      delete_key = pair.first;
      delete_value = pair.second;
      has_value = true;
      DeltaNode::Initialize(NodeType::delete_node, n->GetSize() - 1, n);
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

    inline void Initialize(const DataPairType &pair, Node *n) {
      update_key = pair.first;
      update_value = pair.second;
      DeltaNode::Initialize(NodeType::update_node, n->GetSize(), n);
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

    inline void Initialize(const KeyType &key, PID pid, unsigned short s, Node *n) {
      split_key = key;
      side = pid;
      DeltaNode::Initialize(NodeType::split_node, s, n);
    }

    inline KeyType GetKey() const {
      return split_key;
    }
  };

  /// Extended structure of a delta node in memory. Contains a key range
  /// [min_key, max_key) and a logical pointer to the leaf.
  struct SeparatorNode : public DeltaNode {
    typedef typename AllocType::template rebind<SeparatorNode>::other alloc_type;

    KeyType min_key;
    KeyType max_key;
    PID leaf;

    inline void Initialize(const KeyType &left_key, const KeyType &right_key, const PID pid, Node *n) {
      min_key = left_key;
      max_key = right_key;
      leaf = pid;
      DeltaNode::Initialize(NodeType::separator_node, n->GetSize() + 1, n);
    }
  };

  struct MappingTable {

    Node** table = new Node*[MAPPING_TABLE_SIZE]();

    inline void Initialize() {
      std::fill_n(table, MAPPING_TABLE_SIZE, 0);
    }

    // Atomically update the value using CAS
    inline void Update(PID key, Node* value) {
      for(;;) {
        Node *head = table[key];
        if(__sync_bool_compare_and_swap(&table[key], table[key], value) == true) {
          static_cast<DeltaNode *>(value)->SetBase(head);
          break;  // Update success
        }
      }
//      table[key] = value;
    }

    // Mark as null if remove is called
    inline void Remove(PID key) {
      for(;;) {
        if(__sync_bool_compare_and_swap(&table[key], table[key], NULL) == true) {
          break;  // Update success
        }
      }
//      table[key] = 0;
    }

    inline int GetSize() {
      return MAPPING_TABLE_SIZE;
    }

    // Get physical pointer from PID
    inline Node* Get(PID key) {
      return table[key];
    }

    // This will be changed if we will not use array
    inline bool ContainsValue(PID key) {
      if(table[key] == 0) {
        return false;
      } else {
        return true;
      }
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
  inline PID AllocateLeaf() {
    LeafNode *n = new (LeafNodeAllocator().allocate(1)) LeafNode();
    n->Initialize();
    PID pid = AllocatePID();
    mapping_table.Update(pid, n);
    return pid;
  }

  /// Allocate and initialize an inner node
  inline PID AllocateInner(unsigned short level) {
    InnerNode *n = new (InnerNodeAllocator().allocate(1)) InnerNode();
    n->Initialize(level);
    PID pid = AllocatePID();
    mapping_table.Update(pid, n);
    return pid;
  }

  /// Allocate and initialize an insert delta node
  inline InsertNode *AllocateInsert(const DataPairType &pair, Node *base) {
    InsertNode *n = new (InsertNodeAllocator().allocate(1)) InsertNode();
    n->Initialize(pair, base);
    return n;
  }

  /// Allocate and initialize an delete delta node
  inline DeleteNode *AllocateDeleteNoValue(const KeyType &key, Node *base) {
    DeleteNode *n = new (DeleteNodeAllocator().allocate(1)) DeleteNode();
    n->InitializeNoValue(key, base);
    return n;
  }

  /// Allocate and initialize an delete delta node
  inline DeleteNode *AllocateDeleteWithValue(const DataPairType &key, Node *base) {
    DeleteNode *n = new (DeleteNodeAllocator().allocate(1)) DeleteNode();
    n->InitializeWithValue(key, base);
    return n;
  }

  /// Allocate and initialize an insert delta node
  inline UpdateNode *AllocateUpdate(const DataPairType &pair, Node *base) {
    UpdateNode *n = new (UpdateNodeAllocator().allocate(1)) UpdateNode();
    n->Initialize(pair, base);
    return n;
  }

  /// Allocate and initialize an split delta node
  inline SplitNode *AllocateSplit(KeyType &key, PID leaf, unsigned short size, Node *base) {
    SplitNode *n = new (SplitNodeAllocator().allocate(1)) SplitNode();
    n->Initialize(key, leaf, size, base);
    return n;
  }

  /// Allocate and initialize an separator delta node
  inline SeparatorNode *AllocateSeparator(KeyType &left_key, KeyType &right_key, PID leaf, Node *base) {
    SeparatorNode *n = new (SeparateNodeAllocator().allocate(1)) SeparatorNode();
    n->Initialize(left_key, right_key, leaf, base);
    return n;
  }

  /// Correctly free either inner or leaf node, destructs all contained key
  /// and value objects & frees delta nodes
  inline void FreeNode(Node *n)
  {
    if (n->IsLeaf()) {
      LeafNode *ln = static_cast<LeafNode*>(n);
      typename LeafNode::alloc_type a(LeafNodeAllocator());
      a.destroy(ln);
      a.deallocate(ln, 1);
    }
    else if(n->IsDelta()) {
      if(n->GetType() == NodeType::delete_node) {
        DeleteNode *del = static_cast<DeleteNode *>(n);
        typename DeleteNode::alloc_type a(DeleteNodeAllocator());
        a.destroy(del);
        a.deallocate(del, 1);
      }
      else if(n->GetType() == NodeType::insert_node) {
        InsertNode *ins = static_cast<InsertNode*>(n);
        typename InsertNode::alloc_type a(InsertNodeAllocator());
        a.destroy(ins);
        a.deallocate(ins,1);
      }
    }
    else {
      InnerNode *inner = static_cast<InnerNode*>(n);
      typename InnerNode::alloc_type a(InnerNodeAllocator());
      a.destroy(inner);
      a.deallocate(inner, 1);
    }
  }

public:
  void Clear() {
    if(m_root) {
      ClearRecursive(m_root);
    }

  }


private:

  void ClearRecursive(PID pid) {
    Node* node = mapping_table.Get(pid);
    while(node->IsDelta()) {
      Node* prev= node;
      node = static_cast<DeltaNode *>(node)->GetBase();
      FreeNode(prev);
    }

    if(node->IsLeaf()) {
      LeafNode* leaf_node = static_cast<LeafNode*>(node);
      FreeNode(leaf_node);

    } else {
      InnerNode* inner_node = static_cast<InnerNode *>(node);
      for (unsigned short slot = 0; slot < inner_node->slot_use + 1; ++slot)
      {
        ClearRecursive(inner_node->child_pid[slot]);
        FreeNode(inner_node);
      }

    }
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

  inline void SetNode(PID pid, Node *n) {
    mapping_table.Update(pid, n);
  }

private:
  inline unsigned short FindLower(const InnerNode *n, const KeyType &key) const {
    unsigned short lo = 0;
    while (lo < n->slot_use && KeyLess(n->slot_key[lo], key)) ++lo;
    return lo;
  }

  inline Node *GetBaseNode(Node *n) const {
    while (n->IsDelta()) {
      n = static_cast<DeltaNode *>(n)->GetBase();
    }
    return n;
  }

  inline std::vector<DataPairType> GetAllData(Node *n) const {
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

    std::vector<DataPairType> result;
    for (int i = 0; i < inserted.size(); i++)
      result.push_back(inserted[i]);

    for (unsigned short slot = 0; slot < n->GetSize(); slot++) {
      result.push_back(std::make_pair(static_cast<LeafNode *>(n)->slot_key[slot], static_cast<LeafNode *>(n)->slot_data[slot]));
    }

    if(result.size() == 0) {
      return result;
    }

    // std::sort(result.begin(), result.end(), data_comparator);
    for (int i = 0; i < result.size() - 1; i++)
      for (int j = i + 1; j < result.size(); j++) {
        if (KeyGreater(result[i].first, result[j].first)) {
          DataPairType tmp = result[i];
          result[i] = result[j];
          result[j] = tmp;
        }
      }
    return result;
  }

  // Helper function for checking if the key is in the vector.
  inline bool VectorContainsKey(std::vector<DataPairType> data, const KeyType &key) const {
    for (auto it = data.begin() ; it != data.end(); ++it) {
      if(KeyEqual(key, it->first)) {
        return true;
      }
    }
    return false;
  }


  // Helper function for checking if the data is in the vector.
  inline bool VectorContainsData(std::vector<DataPairType> data, const DataPairType &pair) const {
    KeyType key = pair.first;
    ValueType value = pair.second;
    for (auto it = data.begin() ; it != data.end(); ++it) {
      if(KeyEqual(key, it->first)
        && value.block == (it->second).block
        && value.offset == (it->second).offset) {

        return true;
      }
    }
    return false;
  }

  // Helper function for checking if the key is in the vector.
  inline bool KeyVectorContainsKey(std::vector<KeyType> keys, KeyType &key) const {
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
      NodeType current_type = current->GetType();
      // We need to take care of delta nodes from split/merge and regular inner node
      switch(current_type) {

        case NodeType::insert_node:
          break;
        case NodeType::delete_node:
          break;
        case NodeType::update_node:
          break;
        case NodeType::split_node:
          break;
        case NodeType::separator_node :
          break;

        case NodeType::leaf_node:
          break;
        case NodeType::inner_node :
          const InnerNode* current_inner = static_cast<const InnerNode *>(current);
          int slot = FindLower(current_inner, key);
          current_pid = current_inner->child_pid[slot];
          current = mapping_table.Get(current_pid);
          break;
      }
    }

    return current_pid;
  }

private:
  size_t Count(const KeyType &key);
  // Split/ Merge are internal
  void SplitLeaf(PID pid);

public:
  // BW Tree API
  void InsertData(const DataPairType &x);
  void DeleteKey(const KeyType &x);
  void DeleteData(const DataPairType &x);
  void UpdateData(const DataPairType &x);
  bool Exists(const KeyType &key);
  std::vector<DataPairType> Search(const KeyType &key);
  std::vector<DataPairType> SearchAll();
  std::vector<DataPairType> SearchRange(const KeyType &low_key, const KeyType &high_key);


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
