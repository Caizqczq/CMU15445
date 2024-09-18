#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  auto node = root_;
  for (auto ch : key) {
    // 找不到
    if (node == nullptr || node->children_.find(ch) == node->children_.end()) {
      return nullptr;
    }
    // 找到了
    node = node->children_.at(ch);
  }

  const auto ret_node = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());

  // 转换失败
  if (ret_node == nullptr) {
    return nullptr;
  }
  return ret_node->value_.get();

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
void PutCycle(std::shared_ptr<TrieNode> &new_root, std::string_view key, T value) {
  bool flag = false;
  for (auto &pair : new_root->children_) {
    if (key.at(0) == pair.first) {
      flag = true;
      // 剩余键长度大于1
      if (key.size() > 1) {
        std::shared_ptr<TrieNode> ptr = pair.second->Clone();
        // 递归写入
        PutCycle(ptr, key.substr(1), std::move(value));
        // 覆盖原有子结点
        pair.second = std::shared_ptr<TrieNode>(ptr);
      } else {
        std::shared_ptr<T> val_p = std::make_shared<T>(std::move(value));
        TrieNodeWithValue node_with_value(pair.second->children_, val_p);
        pair.second = std::make_shared<TrieNodeWithValue<T>>(node_with_value);
      }
      return;
    }
  }
  // 若没找到,则新建一个结点
  if (!flag) {
    char c = key.at(0);
    if (key.size() == 1) {
      // 直接插入
      auto val_p = std::make_shared<T>(std::move(value));
      new_root->children_.insert({c, std::make_shared<TrieNodeWithValue<T>>(val_p)});
    } else {
      auto ptr = std::make_shared<TrieNode>();
      // 递归插入
      PutCycle(ptr, key.substr(1), std::move(value));
      new_root->children_.insert({c, std::move(ptr)});
    }
  }
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  if (key.empty()) {
    // 新的值结点
    std::shared_ptr<T> val_p = std::make_shared<T>(std::move(value));
    // 新的根结点
    std::unique_ptr<TrieNodeWithValue<T>> new_root = nullptr;
    // 若原根结点无子节点
    if (root_->children_.empty()) {
      // 直接修改根节点
      new_root = std::make_unique<TrieNodeWithValue<T>>(std::move(val_p));
    } else {
      new_root = std::make_unique<TrieNodeWithValue<T>>(root_->children_, std::move(val_p));
    }

    return Trie(std::move(new_root));
  }
  // 第二种情况,key不等于0,值不放在根结点中
  std::shared_ptr<TrieNode> new_root = nullptr;
  if (root_ == nullptr) {
    new_root = std::make_unique<TrieNode>();
  } else {
    new_root = root_->Clone();
  }

  PutCycle(new_root, key, std::move(value));

  return Trie(std::move(new_root));

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto RemoveCycle(const std::shared_ptr<TrieNode> &new_rootty, std::string_view key) {
  for (auto &pair : new_rootty->children_) {
    if (key.at(0) != pair.first) {
      continue;
    }
    if (key.size() == 1) {
      if (!pair.second->is_value_node_) {
        return false;
      }
      if (pair.second->children_.empty()) {
        new_rootty->children_.erase(pair.first);
      } else {
        pair.second = std::make_shared<const TrieNode>(pair.second->children_);
      }
      return true;
    }
    std::shared_ptr<TrieNode> ptr = pair.second->Clone();
    bool flag = RemoveCycle(ptr, key.substr(1, key.size() - 1));
    if (!flag) {
      return false;
    }
    if (ptr->children_.empty() && !ptr->is_value_node_) {
      new_rootty->children_.erase(pair.first);
    } else {
      pair.second = std::shared_ptr<TrieNode>(ptr);
    }
    return true;
  }
  return false;
}

auto Trie::Remove(std::string_view key) const -> Trie {
 if (this->root_ == nullptr) {
    return *this;
  }
  // key是空的
  if (key.empty()) {
    if (root_->is_value_node_) {
      if (root_->children_.empty()) {
        return Trie(nullptr);
      }
      // 根节点有子节点,将子节点转换为新根
      std::shared_ptr<TrieNode> new_root = std::make_shared<TrieNode>(root_->children_);
      return Trie(new_root);
    }
    // 若根节点无值,直接返回
    return *this;
  }

  // key不是空的
  // 创建一个新的根节点
  std::shared_ptr<TrieNode> new_root = root_->Clone();

  // 进行递归的删除操作
  bool flag = RemoveCycle(new_root, key);
  if (!flag) {
    return *this;
  }
  if (new_root->children_.empty() && !new_root->is_value_node_) {
    new_root = nullptr;
  }

  return Trie(std::move(new_root));

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}


// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
