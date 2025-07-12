// Copyright (c) 2025 RISC Zero, Inc.
//
// All rights reserved.

use std::sync::Arc;
use std::time::Duration;

use alloy::{
    network::Ethereum,
    primitives::Address,
    providers::{Provider, WalletProvider},
};
use anyhow::{Context, Result};
use boundless_market::contracts::boundless_market::BoundlessMarketService;
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{
    chain_monitor::ChainMonitorService,
    config::ConfigLock,
    db::DbObj,
    errors::{impl_coded_debug, CodedError},
    market_monitor::MarketMonitor,
    order_picker::OrderPicker,
    now_timestamp,
    OrderRequest,
};

#[derive(Error)]
pub enum ParallelOrderPickerErr {
    #[error("{code} Failed to lock order: {0}", code = self.code())]
    LockOrderFailed(String),

    #[error("{code} Unexpected error: {0:?}", code = self.code())]
    UnexpectedError(#[from] anyhow::Error),
}

impl_coded_debug!(ParallelOrderPickerErr);

impl CodedError for ParallelOrderPickerErr {
    fn code(&self) -> &str {
        match self {
            ParallelOrderPickerErr::LockOrderFailed(_) => "[B-POP-001]",
            ParallelOrderPickerErr::UnexpectedError(_) => "[B-POP-500]",
        }
    }
}

pub struct ParallelOrderPicker<P> {
    db: DbObj,
    config: ConfigLock,
    provider: Arc<P>,
    #[allow(dead_code)]
    chain_monitor: Arc<ChainMonitorService<P>>,
    market_addr: Address,
    priced_orders_tx: mpsc::Sender<Box<OrderRequest>>,
    #[allow(dead_code)]
    order_picker: Arc<OrderPicker<P>>,
    market_monitor: Arc<MarketMonitor<P>>,
    #[allow(dead_code)]
    market: BoundlessMarketService<Arc<P>>,
    concurrency: usize,
}

impl<P> ParallelOrderPicker<P>
where
    P: Provider<Ethereum> + WalletProvider + Clone + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: DbObj,
        config: ConfigLock,
        provider: Arc<P>,
        chain_monitor: Arc<ChainMonitorService<P>>,
        market_addr: Address,
        priced_orders_tx: mpsc::Sender<Box<OrderRequest>>,
        order_picker: Arc<OrderPicker<P>>,
        market_monitor: Arc<MarketMonitor<P>>,
        concurrency: usize,
    ) -> Self {
        let market = BoundlessMarketService::new(
            market_addr,
            provider.clone(),
            provider.default_signer_address(),
        );

        Self {
            db,
            config,
            provider,
            chain_monitor,
            market_addr,
            priced_orders_tx,
            order_picker,
            market_monitor,
            market,
            concurrency,
        }
    }

    pub async fn start(self: Arc<Self>, cancel_token: CancellationToken) -> Result<()> {
        tracing::info!("启动高并发订单获取器，并发级别: {}", self.concurrency);
        
        loop {
            if cancel_token.is_cancelled() {
                break;
            }

            // 获取可用订单
            match self.market_monitor.get_available_orders().await {
                Ok(orders) => {
                    if !orders.is_empty() {
                        tracing::info!("发现 {} 个可用订单，开始并行处理", orders.len());
                        self.process_orders_in_parallel(orders).await?;
                    }
                }
                Err(e) => {
                    tracing::warn!("获取可用订单失败: {:?}", e);
                }
            }

            // 短暂等待后再次尝试
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Ok(())
    }

    async fn process_orders_in_parallel(&self, orders: Vec<Box<OrderRequest>>) -> Result<()> {
        let mut join_set = JoinSet::new();
        let orders_mutex = Arc::new(Mutex::new(orders));
        
        // 启动并发任务
        for _ in 0..self.concurrency {
            // 克隆需要的组件而不是整个 self
            let db = self.db.clone();
            let config = self.config.clone();
            let provider = self.provider.clone();
            let priced_orders_tx = self.priced_orders_tx.clone();
            let market = BoundlessMarketService::new(
                self.market_addr,
                provider.clone(),
                provider.default_signer_address(),
            );
            let orders_mutex = orders_mutex.clone();
            
            join_set.spawn(async move {
                loop {
                    // 获取下一个订单
                    let order = {
                        let mut orders = orders_mutex.lock().await;
                        if orders.is_empty() {
                            return Ok::<_, ParallelOrderPickerErr>(());
                        }
                        orders.pop().unwrap()
                    };
                    
                    let request_id = order.request.id;
                    
                    // 处理单个订单
                    match process_single_order(
                        &db, 
                        &config, 
                        &market, 
                        &priced_orders_tx, 
                        &order
                    ).await {
                        Ok(true) => {
                            tracing::trace!("！！！成功锁定订单！！！: 0x{:x}", request_id);
                        }
                        Ok(false) => {
                            tracing::debug!("订单已被锁定: 0x{:x}", request_id);
                        }
                        Err(e) => {
                            tracing::warn!("处理订单失败 0x{:x}: {:?}", request_id, e);
                        }
                    }
                }
            });
        }

        // 等待所有任务完成
        while let Some(result) = join_set.join_next().await {
            result??;
        }

        Ok(())
    }
}

// 将处理逻辑移到独立的函数中，避免生命周期问题
async fn process_single_order<P>(
    db: &DbObj,
    config: &ConfigLock,
    market: &BoundlessMarketService<Arc<P>>,
    priced_orders_tx: &mpsc::Sender<Box<OrderRequest>>,
    order: &Box<OrderRequest>,
) -> Result<bool, ParallelOrderPickerErr>
where
    P: Provider<Ethereum> + WalletProvider + Clone + 'static,
{
    // 获取订单ID
    let request_id = order.request.id;
    
    // 直接尝试锁定订单，不进行任何预检查
    // 获取Gas价格配置并直接使用
    let conf_priority_gas = {
        let conf = config.lock_all()
            .context("Failed to lock config")?;
        // 直接使用配置的值，不做额外乘法
        conf.market.lockin_priority_gas
    };
    
    // 立即尝试锁定订单
    tracing::info!(
        "快速锁单: 尝试锁定 0x{:x}, 出价: {}",
        request_id,
        order.request.offer.lockStake
    );
    
    // 直接锁定，不进行前置检查
    match market.lock_request(&order.request, order.client_sig.clone(), conf_priority_gas).await {
        Ok(_) => {
            // 获取锁定时的价格
            let now = now_timestamp();
            let lock_price = order.request.offer.price_at(now)
                .context("Failed to calculate lock price")?;
            
            // 将订单插入到数据库
            if let Err(e) = db.insert_accepted_request(&order, lock_price).await {
                tracing::warn!("订单已锁定但数据库更新失败: {}", e);
            }
            
            // 发送到订单处理通道
            let mut order_clone = (**order).clone(); // 先解引用Box<OrderRequest>再克隆
            order_clone.target_timestamp = Some(now);
            if let Err(e) = priced_orders_tx.send(Box::new(order_clone)).await {
                tracing::warn!("订单已锁定但发送到处理通道失败: {}", e);
            }
            
            Ok(true)
        },
        Err(e) => {
            let error_msg = e.to_string();
            
            // 根据错误信息分析失败原因并提供更详细的日志
            if error_msg.contains("RequestIsLocked") || error_msg.contains("AlreadyLocked") || error_msg.contains("Request already locked") {
                // 订单已被其他节点锁定
                tracing::info!("订单 0x{:x} 已被其他节点锁定", request_id);
                Ok(false)
            } else if error_msg.contains("RequestIsFulfilled") {
                // 订单已经被完成
                tracing::info!("订单 0x{:x} 已经被完成，不需要锁定", request_id);
                Ok(false)
            } else if error_msg.contains("RequestLockIsExpired") {
                // 订单锁定期已过期
                tracing::info!("订单 0x{:x} 锁定期已过期", request_id);
                Ok(false)
            } else if error_msg.contains("InsufficientBalance") {
                // 余额不足
                tracing::warn!("锁定订单 0x{:x} 失败: 余额不足，请检查账户余额", request_id);
                Err(ParallelOrderPickerErr::LockOrderFailed(format!("余额不足: {}", error_msg)))
            } else if error_msg.contains("LockRevert") {
                // 交易被回滚
                tracing::warn!("锁定订单 0x{:x} 失败: 交易被回滚，可能是被其他节点抢先锁定", request_id);
                Err(ParallelOrderPickerErr::LockOrderFailed(format!("交易被回滚: {}", error_msg)))
            } else {
                // 其他错误
                tracing::warn!("锁定订单 0x{:x} 失败: 未知错误: {}", request_id, error_msg);
                Err(ParallelOrderPickerErr::LockOrderFailed(error_msg))
            }
        }
    }
} 