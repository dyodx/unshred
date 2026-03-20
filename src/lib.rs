mod config;
mod processor;
mod receiver;
mod types;

#[cfg(feature = "metrics")]
mod metrics;

#[cfg(feature = "metrics")]
use std::sync::Arc;

pub use config::UnshredConfig;

use anyhow::Result;
use solana_transaction::versioned::VersionedTransaction;

use crate::processor::ShredProcessor;

#[derive(Debug)]
pub struct TransactionEvent<'a> {
    pub slot: u64,
    pub transaction: &'a VersionedTransaction,
    /// * `Some(_)` - data shred containing this transaction was directly received via UDP
    /// * `None`    - data shred containing this transaction was recovered via code shreds
    ///
    /// Note: Estimated as the received_at_micros of the data shred that contained
    ///       the first byte of the Entry that contained this transaction.
    pub received_at_micros: Option<u64>,
    pub processed_at_micros: u64,
}

pub trait TransactionHandler: Send + Sync + 'static {
    /// Called for each reconstructed transaction
    /// # Returns
    /// * `Ok(())` - to continue processing
    /// * `Err(_)` - to log error and continue (does not stop processing)
    fn handle_transaction(&self, event: &TransactionEvent) -> Result<()>;
}

pub struct UnshredProcessor<H: TransactionHandler> {
    handler: H,
    config: UnshredConfig,
}

impl<H: TransactionHandler> UnshredProcessor<H> {
    pub fn builder() -> UnshredProcessorBuilder<H> {
        UnshredProcessorBuilder::new()
    }

    pub async fn run(self) -> Result<()> {
        let processor = ShredProcessor::new();
        processor.run(self.handler, &self.config).await
    }
}

pub struct UnshredProcessorBuilder<H> {
    handler: Option<H>,
    config: Option<UnshredConfig>,
    #[cfg(feature = "metrics")]
    metrics_registry: Option<Arc<prometheus::Registry>>,
}

impl<H: TransactionHandler> UnshredProcessorBuilder<H> {
    pub fn new() -> Self {
        Self {
            handler: None,
            config: None,
            #[cfg(feature = "metrics")]
            metrics_registry: None,
        }
    }

    pub fn handler(mut self, handler: H) -> Self {
        self.handler = Some(handler);
        self
    }

    pub fn config(mut self, config: UnshredConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn bind_address(mut self, addr: impl Into<String>) -> Self {
        let mut config = self.config.unwrap_or_default();
        config.bind_address = addr.into();
        self.config = Some(config);
        self
    }

    pub fn num_fec_workers(mut self, num: u8) -> Self {
        let mut config = self.config.unwrap_or_default();
        config.num_fec_workers = Some(num);
        self.config = Some(config);
        self
    }

    pub fn num_batch_workers(mut self, num: u8) -> Self {
        let mut config = self.config.unwrap_or_default();
        config.num_batch_workers = Some(num);
        self.config = Some(config);
        self
    }

    #[cfg(feature = "metrics")]
    /// Sets the Prometheus registry for metrics. `features = ["metrics"]` must be enabled.
    pub fn metrics_registry(mut self, registry: Arc<prometheus::Registry>) -> Self {
        self.metrics_registry = Some(registry);
        self
    }

    pub fn build(self) -> Result<UnshredProcessor<H>> {
        let handler = self
            .handler
            .ok_or_else(|| anyhow::anyhow!("Handler is required"))?;
        let config = self.config.unwrap_or_default();

        #[cfg(feature = "metrics")]
        {
            if let Some(registry) = self.metrics_registry {
                crate::metrics::Metrics::init_with_registry(registry)?;
            } else {
                crate::metrics::Metrics::init_disabled()?;
            }
        }

        Ok(UnshredProcessor { handler, config })
    }
}
