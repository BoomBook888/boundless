// Copyright (c) 2025 RISC Zero, Inc.
//
// All rights reserved.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use alloy::primitives::U256;

/// 订单缓存结构，用于存储已处理的订单ID及过期时间
pub struct OrderCache {
    // 存储订单ID和过期时间
    cache: Arc<Mutex<HashMap<U256, Instant>>>,
    // 缓存条目的过期时间（60分钟）
    expiry_duration: Duration,
}

impl OrderCache {
    /// 创建新的订单缓存实例
    pub fn new() -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
            expiry_duration: Duration::from_secs(60 * 60), // 60分钟
        }
    }

    /// 向缓存添加订单ID
    pub fn add(&self, order_id: U256) {
        let mut cache = self.cache.lock().unwrap();
        cache.insert(order_id, Instant::now());
        tracing::debug!("订单ID 0x{:x} 已添加至缓存", order_id);
    }

    /// 检查订单ID是否在缓存中且未过期
    pub fn contains(&self, order_id: &U256) -> bool {
        let cache = self.cache.lock().unwrap();
        if let Some(time) = cache.get(order_id) {
            if time.elapsed() < self.expiry_duration {
                return true;
            }
        }
        false
    }

    /// 清理已过期的订单ID
    pub fn cleanup(&self) -> usize {
        let mut cache = self.cache.lock().unwrap();
        let before_count = cache.len();
        cache.retain(|_, time| time.elapsed() < self.expiry_duration);
        let removed = before_count - cache.len();
        
        if removed > 0 {
            tracing::debug!("已从缓存中清理 {} 个过期的订单ID", removed);
        }
        
        removed
    }

    /// 获取当前缓存中的订单数量
    pub fn len(&self) -> usize {
        let cache = self.cache.lock().unwrap();
        cache.len()
    }

    /// 检查缓存是否为空
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 克隆缓存实例
    pub fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            expiry_duration: self.expiry_duration,
        }
    }
}

impl Default for OrderCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_add_and_contains() {
        let cache = OrderCache::new();
        let order_id = U256::from(123);
        
        // 添加订单ID并检查
        cache.add(order_id);
        assert!(cache.contains(&order_id));
        
        // 不存在的订单ID
        let non_existent = U256::from(456);
        assert!(!cache.contains(&non_existent));
    }

    #[test]
    fn test_expiry() {
        let cache = OrderCache::new();
        let order_id = U256::from(123);
        
        // 设置测试用的短期过期时间
        let test_cache = OrderCache {
            cache: cache.cache.clone(),
            expiry_duration: Duration::from_millis(10),
        };
        
        // 添加订单ID
        test_cache.add(order_id);
        assert!(test_cache.contains(&order_id));
        
        // 等待过期
        thread::sleep(Duration::from_millis(20));
        assert!(!test_cache.contains(&order_id));
    }

    #[test]
    fn test_cleanup() {
        let cache = OrderCache::new();
        let test_cache = OrderCache {
            cache: cache.cache.clone(),
            expiry_duration: Duration::from_millis(10),
        };
        
        // 添加几个订单ID
        test_cache.add(U256::from(1));
        test_cache.add(U256::from(2));
        test_cache.add(U256::from(3));
        
        // 确认都在缓存中
        assert_eq!(test_cache.len(), 3);
        
        // 等待过期
        thread::sleep(Duration::from_millis(20));
        
        // 清理过期项
        let removed = test_cache.cleanup();
        assert_eq!(removed, 3);
        assert!(test_cache.is_empty());
    }
} 