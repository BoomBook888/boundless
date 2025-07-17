// Copyright (c) 2025 RISC Zero, Inc.
//
// All rights reserved.

use std::sync::Arc;

use alloy::{
    network::Ethereum,
    primitives::{Address, U256},
    providers::Provider,
    rpc::types::Filter,
    sol_types::SolEvent,
};

use anyhow::{Context, Result};
use boundless_market::contracts::{
    boundless_market::BoundlessMarketService, IBoundlessMarket, RequestStatus,
};
use tokio::{sync::mpsc, time::{interval, Duration}};
use tokio_util::sync::CancellationToken;

use crate::{
    chain_monitor::ChainMonitorService,
    errors::{impl_coded_debug, CodedError},
    order_cache::OrderCache,
    FulfillmentType, OrderRequest,
    task::{RetryRes, RetryTask, SupervisorErr},
};
use thiserror::Error;

const SCAN_BLOCK_COUNT: u64 = 50;
const SCAN_INTERVAL_MS: u64 = 1000; // 1秒

#[derive(Error)]
pub enum BlockScannerErr {
    #[error("{code} 区块扫描失败: {0:?}", code = self.code())]
    ScanningError(anyhow::Error),

    #[error("{code} 订单处理失败: {0:?}", code = self.code())]
    OrderProcessingFailed(anyhow::Error),

    #[error("{code} 意外错误: {0:?}", code = self.code())]
    UnexpectedErr(#[from] anyhow::Error),

    #[error("{code} 接收方已关闭", code = self.code())]
    ReceiverDropped,
}

impl CodedError for BlockScannerErr {
    fn code(&self) -> &str {
        match self {
            BlockScannerErr::ScanningError(_) => "[B-BS-501]",
            BlockScannerErr::OrderProcessingFailed(_) => "[B-BS-502]",
            BlockScannerErr::UnexpectedErr(_) => "[B-BS-500]",
            BlockScannerErr::ReceiverDropped => "[B-BS-503]",
        }
    }
}

impl_coded_debug!(BlockScannerErr);

pub struct BlockScanner<P> {
    market_addr: Address,
    provider: Arc<P>,
    chain_monitor: Arc<ChainMonitorService<P>>,
    new_order_tx: tokio::sync::mpsc::Sender<Box<OrderRequest>>,
    order_cache: Arc<OrderCache>,
}

impl<P> BlockScanner<P>
where
    P: Provider<Ethereum> + 'static + Clone,
{
    pub fn new(
        market_addr: Address,
        provider: Arc<P>,
        chain_monitor: Arc<ChainMonitorService<P>>,
        new_order_tx: tokio::sync::mpsc::Sender<Box<OrderRequest>>,
    ) -> Self {
        Self {
            market_addr,
            provider,
            chain_monitor,
            new_order_tx,
            order_cache: Arc::new(OrderCache::new()),
        }
    }

    /// 扫描最近的区块获取开放的订单
    async fn scan_recent_blocks(&self) -> Result<u64, BlockScannerErr> {
        let current_block = self.chain_monitor.current_block_number().await?;
        let chain_id = self.provider.get_chain_id().await.context("获取链ID失败")?;

        // 计算开始区块 (最多往前查50个区块)
        let start_block = if current_block > SCAN_BLOCK_COUNT {
            current_block - SCAN_BLOCK_COUNT
        } else {
            0
        };

        tracing::debug!("扫描区块 {start_block} - {current_block} 获取开放订单");

        let market = BoundlessMarketService::new(self.market_addr, self.provider.clone(), Address::ZERO);

        // 创建过滤器查询RequestSubmitted事件
        let filter = Filter::new()
            .event_signature(IBoundlessMarket::RequestSubmitted::SIGNATURE_HASH)
            .from_block(start_block)
            .address(self.market_addr);

        // 获取日志
        let logs = self.provider.get_logs(&filter).await.context("获取日志失败")?;
        let decoded_logs = logs.iter().filter_map(|log| {
            match log.log_decode::<IBoundlessMarket::RequestSubmitted>() {
                Ok(res) => Some(res),
                Err(err) => {
                    tracing::error!("解码RequestSubmitted日志失败: {err:?}");
                    None
                }
            }
        });

        tracing::debug!("发现 {} 个可能的订单", logs.len());
        
        let mut order_count = 0;
        for log in decoded_logs {
            let event = &log.inner.data;
            let request_id = U256::from(event.requestId);
            
            // 检查订单是否已在缓存中
            if self.order_cache.contains(&request_id) {
                tracing::debug!("订单 0x{:x} 已处理过，跳过", request_id);
                continue;
            }

            // 检查订单状态
            let req_status =
                match market.get_status(request_id, Some(event.request.expires_at())).await {
                    Ok(val) => val,
                    Err(err) => {
                        tracing::warn!("获取请求状态失败: {err:?}");
                        continue;
                    }
                };

            // 跳过已经不是投标状态的订单
            if !matches!(req_status, RequestStatus::Unknown) {
                tracing::debug!(
                    "跳过订单 0x{:x}，原因: 订单状态不再是投标状态: {:?}",
                    request_id,
                    req_status
                );
                continue;
            }

            // 根据状态选择履行类型
            let fulfillment_type = match req_status {
                RequestStatus::Locked => FulfillmentType::FulfillAfterLockExpire,
                _ => FulfillmentType::LockAndFulfill,
            };

            tracing::info!(
                "发现开放订单: 0x{:x}，状态: {:?}，准备使用履行类型: {:?} 进行处理",
                request_id,
                req_status,
                fulfillment_type
            );

            // 创建新订单并发送处理
            let new_order = OrderRequest::new(
                event.request.clone(),
                event.clientSignature.clone(),
                fulfillment_type,
                self.market_addr,
                chain_id,
            );

            // 添加到缓存
            self.order_cache.add(request_id);

            // 发送到处理通道
            self.new_order_tx
                .send(Box::new(new_order))
                .await
                .map_err(|_| BlockScannerErr::ReceiverDropped)?;
            
            order_count += 1;
        }

        if order_count > 0 {
            tracing::info!("本次扫描发现 {order_count} 个新开放订单");
        }

        // 清理过期的订单缓存
        let removed = self.order_cache.cleanup();
        if removed > 0 {
            tracing::debug!("已清理 {removed} 个过期的订单缓存项");
        }

        Ok(order_count)
    }

    /// 运行区块扫描器，每秒扫描一次
    async fn run_scanner(
        market_addr: Address,
        provider: Arc<P>,
        chain_monitor: Arc<ChainMonitorService<P>>,
        new_order_tx: mpsc::Sender<Box<OrderRequest>>,
        order_cache: Arc<OrderCache>,
        cancel_token: CancellationToken,
    ) -> Result<(), BlockScannerErr> {
        let mut interval = interval(Duration::from_millis(SCAN_INTERVAL_MS));
        
        // 创建区块扫描器实例
        let scanner = BlockScanner {
            market_addr,
            provider,
            chain_monitor,
            new_order_tx,
            order_cache,
        };

        tracing::info!("区块扫描器已启动，将每 {} 毫秒扫描最新的 {} 个区块", SCAN_INTERVAL_MS, SCAN_BLOCK_COUNT);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // 扫描区块
                    if let Err(err) = scanner.scan_recent_blocks().await {
                        tracing::error!("区块扫描失败: {err:?}");
                    }
                }
                _ = cancel_token.cancelled() => {
                    tracing::info!("区块扫描器收到取消信号，正在退出");
                    return Ok(());
                }
            }
        }
    }
}

impl<P> RetryTask for BlockScanner<P>
where
    P: Provider<Ethereum> + 'static + Clone,
{
    type Error = BlockScannerErr;

    fn spawn(&self, cancel_token: CancellationToken) -> RetryRes<Self::Error> {
        let market_addr = self.market_addr;
        let provider = self.provider.clone();
        let chain_monitor = self.chain_monitor.clone();
        let new_order_tx = self.new_order_tx.clone();
        let order_cache = self.order_cache.clone();

        Box::pin(async move {
            tracing::info!("启动区块扫描器");

            Self::run_scanner(
                market_addr,
                provider,
                chain_monitor,
                new_order_tx,
                order_cache,
                cancel_token,
            )
            .await
            .map_err(SupervisorErr::Recover)?;

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SqliteDb;
    use alloy::{
        network::EthereumWallet,
        node_bindings::Anvil,
        providers::{ProviderBuilder, WalletProvider},
        signers::local::PrivateKeySigner,
    };
    use boundless_market::contracts::boundless_market::BoundlessMarketService;
    use boundless_market_test_utils::{deploy_boundless_market, ASSESSOR_GUEST_ID, ASSESSOR_GUEST_PATH};
    use risc0_zkvm::sha::Digest;

    #[tokio::test]
    async fn test_block_scanner() {
        let anvil = Anvil::new().spawn();
        let signer: PrivateKeySigner = anvil.keys()[0].clone().into();
        let provider = Arc::new(
            ProviderBuilder::new()
                .wallet(EthereumWallet::from(signer.clone()))
                .connect(&anvil.endpoint())
                .await
                .unwrap(),
        );

        // 部署市场合约
        let market_address = deploy_boundless_market(
            signer.address(),
            provider.clone(),
            Address::ZERO,
            Address::ZERO,
            Digest::from(ASSESSOR_GUEST_ID),
            format!("file://{ASSESSOR_GUEST_PATH}"),
            Some(signer.address()),
        )
        .await
        .unwrap();

        // 创建链监控服务
        let chain_monitor = Arc::new(ChainMonitorService::new(provider.clone()).await.unwrap());
        tokio::spawn(chain_monitor.spawn(Default::default()));

        // 创建通道
        let (order_tx, mut order_rx) = tokio::sync::mpsc::channel(16);

        // 创建区块扫描器
        let scanner = BlockScanner::new(market_address, provider, chain_monitor, order_tx);

        // 测试扫描方法
        let orders = scanner.scan_recent_blocks().await.unwrap();
        
        // 确认没有找到订单（因为我们没有提交任何订单）
        assert_eq!(orders, 0);
        
        // 确认接收通道是空的
        assert!(order_rx.try_recv().is_err());
    }
} 