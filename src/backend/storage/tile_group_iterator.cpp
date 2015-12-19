//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tile_group_iterator.cpp
//
// Identification: src/backend/storage/tile_group_iterator.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/storage/tile_group_iterator.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"

namespace peloton {
namespace storage {

bool TileGroupIterator::Next(std::shared_ptr<TileGroup> &tileGroup) {
  if (HasNext()) {
    auto next = table_->GetTileGroup(tile_group_itr_);
    tileGroup.swap(next);
    tile_group_itr_++;
    return (true);
  }
  return (false);
}

bool TileGroupIterator::HasNext() {
  return (tile_group_itr_ < table_->GetTileGroupCount());
}

}  // End storage namespace
}  // End peloton namespace
