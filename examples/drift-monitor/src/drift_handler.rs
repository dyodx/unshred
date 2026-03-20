use std::sync::Arc;

use anyhow::Result;
use prometheus::{register_int_counter_vec_with_registry, IntCounterVec, Registry};
use solana_pubkey::Pubkey;
use solana_transaction::versioned::VersionedTransaction;
use tokio::sync::mpsc::Sender;
use unshred::{TransactionEvent, TransactionHandler};

use crate::types::DriftEvent;

pub struct DriftHandler {
    drift_pubkey: Pubkey,
    event_sender: Sender<DriftEvent>,

    // Metrics
    drift_instructions_processed: IntCounterVec,
}

impl DriftHandler {
    pub fn new(
        drift_program_id: String,
        event_sender: Sender<DriftEvent>,
        registry: &Arc<Registry>,
    ) -> Result<Self> {
        let drift_pubkey = drift_program_id.parse()?;

        let drift_instructions_processed = register_int_counter_vec_with_registry!(
            "drift_instructions_processed_total",
            "Total number of drift instructions processed by liquidation type",
            &["type"],
            registry.clone()
        )?;
        Ok(Self {
            drift_pubkey,
            event_sender,
            drift_instructions_processed,
        })
    }

    fn handle_drift_transaction(&self, event: &TransactionEvent) -> Result<()> {
        for instruction in event.transaction.message.instructions().iter() {
            if let Some(program_id) = event
                .transaction
                .message
                .static_account_keys()
                .get(instruction.program_id_index as usize)
            {
                if *program_id == self.drift_pubkey {
                    let liquidation_type = Self::detect_liquidation(&instruction.data);
                    let liq_type_str = liquidation_type
                        .clone()
                        .unwrap_or_else(|| "other".to_string());

                    let event = DriftEvent {
                        slot: event.slot,
                        signature: bs58::encode(&event.transaction.signatures[0]).into_string(),
                        instruction_data: bs58::encode(&instruction.data).into_string(),
                        accounts: event
                            .transaction
                            .message
                            .static_account_keys()
                            .iter()
                            .map(|k| k.to_string())
                            .collect(),
                        liquidation_type,
                        received_at_micros: event.received_at_micros,
                        processed_at_micros: event.processed_at_micros,
                    };

                    if let Err(e) = self.event_sender.try_send(event) {
                        match e {
                            tokio::sync::mpsc::error::TrySendError::Full(_) => {
                                return Err(anyhow::anyhow!("Event channel full, dropping event"));
                            }
                            tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                                return Err(anyhow::anyhow!("Event channel closed"));
                            }
                        }
                    }
                    self.drift_instructions_processed
                        .with_label_values(&[liq_type_str])
                        .inc();
                }
            }
        }
        Ok(())
    }

    fn is_drift_transaction(&self, tx: &VersionedTransaction) -> bool {
        tx.message.instructions().iter().any(|ix| {
            match tx
                .message
                .static_account_keys()
                .get(ix.program_id_index as usize)
            {
                Some(account_key) => *account_key == self.drift_pubkey,
                None => false,
            }
        })
    }

    /// Detects liquidation instructions using Drift discriminators
    fn detect_liquidation(instruction_data: &[u8]) -> Option<String> {
        if instruction_data.len() < 8 {
            return None;
        }

        let discriminator = [
            instruction_data[0],
            instruction_data[1],
            instruction_data[2],
            instruction_data[3],
            instruction_data[4],
            instruction_data[5],
            instruction_data[6],
            instruction_data[7],
        ];

        match discriminator {
            [107, 0, 128, 41, 35, 229, 251, 18] => Some("liquidate_spot".to_string()),
            [75, 35, 119, 247, 191, 18, 139, 2] => Some("liquidate_perp".to_string()),
            [12, 43, 176, 83, 156, 251, 117, 13] => {
                Some("liquidate_spot_with_swap_begin".to_string())
            }
            [142, 88, 163, 160, 223, 75, 55, 225] => {
                Some("liquidate_spot_with_swap_end".to_string())
            }
            [169, 17, 32, 90, 207, 148, 209, 27] => {
                Some("liquidate_borrow_for_perp_pnl".to_string())
            }
            [95, 111, 124, 105, 86, 169, 187, 34] => Some("liquidate_perp_with_fill".to_string()),
            [13, 248, 54, 57, 214, 231, 166, 184] => {
                Some("liquidate_perp_pnl_for_deposit".to_string())
            }
            [124, 194, 240, 254, 198, 213, 52, 122] => Some("resolve_spot_bankruptcy".to_string()),
            [224, 16, 176, 214, 162, 213, 183, 222] => Some("resolve_perp_bankruptcy".to_string()),
            [106, 133, 160, 206, 193, 171, 192, 194] => {
                Some("set_user_status_to_being_liquidated".to_string())
            }
            _ => None,
        }
    }
}

impl TransactionHandler for DriftHandler {
    fn handle_transaction(&self, event: &TransactionEvent) -> Result<()> {
        if self.is_drift_transaction(event.transaction) {
            self.handle_drift_transaction(event)?;
        }

        Ok(())
    }
}
