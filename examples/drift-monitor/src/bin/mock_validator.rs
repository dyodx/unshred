// Send mock shreds

use anyhow::Result;
use rand::Rng;
use solana_entry::entry::Entry;
use solana_hash::Hash;
use solana_instruction::{AccountMeta, Instruction};
use solana_keypair::Keypair;
use solana_ledger::shred::DATA_SHREDS_PER_FEC_BLOCK;
use solana_message::Message;
use solana_pubkey::Pubkey;
use solana_signature::SIGNATURE_BYTES;
use solana_signer::Signer;
use solana_transaction::{versioned::VersionedTransaction, Transaction};
use std::{
    collections::VecDeque,
    net::UdpSocket,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tracing::{info, warn};
// Mock settings (estimates)
const SLOT_DURATION_MS: u64 = 400;

// Constants
const TICKS_PER_SLOT: u64 = 64;
const FEC_SET_SIZE: u32 = 32;
const MERKLE_DATA_VARIANT: u8 = 0x90;
const MERKLE_CODE_VARIANT: u8 = 0x60;
const MERKLE_DATA_SHRED_SIZE: usize = 1203;
const MERKLE_CODE_SHRED_SIZE: usize = 1228;
const PROOF_HEIGHT: u8 = 6; // log_2(32+32); 32 Data and 32 Coding shreds
const SIZE_OF_MERKLE_PROOF_ENTRY: usize = 20;
const SIZE_OF_MERKLE_ROOT: usize = 32; // Chained merkle root
const MERKLE_PROOF_SIZE: usize = PROOF_HEIGHT as usize * SIZE_OF_MERKLE_PROOF_ENTRY;
// shred size - headers - chained merkle root - merkle proof
const MERKLE_DATA_CAPACITY: usize =
    MERKLE_DATA_SHRED_SIZE - DATA_OFFSET_PAYLOAD - SIZE_OF_MERKLE_ROOT - MERKLE_PROOF_SIZE;

// Estimates
const MIN_TARGET_ENTRIES_PER_BATCH: usize = 1;
const MAX_TARGET_ENTRIES_PER_BATCH: usize = 4;
const MIN_TARGET_TXS_PER_ENTRY: usize = 0;
const MAX_TARGET_TXS_PER_ENTRY: usize = 10;

// Header offsets
const OFFSET_SHRED_VARIANT: usize = 64;
const OFFSET_SHRED_SLOT: usize = 65;
const OFFSET_SHRED_INDEX: usize = 73;
const OFFSET_SHRED_VERSION: usize = 77;
const OFFSET_FEC_SET_INDEX: usize = 79;
const OFFSET_PARENT_OFFSET: usize = 83;
const OFFSET_FLAGS: usize = 85;
const OFFSET_SIZE: usize = 86; // Payload total size offset
const DATA_OFFSET_PAYLOAD: usize = 88; // Common header (83) + Data header (5)
const CODE_OFFSET_PAYLOAD: usize = 89;

const DRIFT_LIQUIDATION_DISCRIMINATORS: &[([u8; 8], &str)] = &[
    ([107, 0, 128, 41, 35, 229, 251, 18], "liquidate_spot"),
    ([75, 35, 119, 247, 191, 18, 139, 2], "liquidate_perp"),
    (
        [12, 43, 176, 83, 156, 251, 117, 13],
        "liquidate_spot_with_swap_begin",
    ),
    (
        [142, 88, 163, 160, 223, 75, 55, 225],
        "liquidate_spot_with_swap_end",
    ),
    (
        [169, 17, 32, 90, 207, 148, 209, 27],
        "liquidate_borrow_for_perp_pnl",
    ),
    (
        [95, 111, 124, 105, 86, 169, 187, 34],
        "liquidate_perp_with_fill",
    ),
    (
        [237, 75, 198, 235, 233, 186, 75, 35],
        "liquidate_perp_pnl_for_deposit",
    ),
    (
        [124, 194, 240, 254, 198, 213, 52, 122],
        "resolve_spot_bankruptcy",
    ),
    (
        [224, 16, 176, 214, 162, 213, 183, 222],
        "resolve_perp_bankruptcy",
    ),
    (
        [106, 133, 160, 206, 193, 171, 192, 194],
        "set_user_status_to_being_liquidated",
    ),
];

pub struct MockValidator {
    start_time: Instant,

    socket: UdpSocket,
    target: String,
    shred_version: u16,
    payer_keypairs: Vec<Keypair>,
    user_keypairs: Vec<Keypair>,
    keypair_index: AtomicUsize,
    drift_program_id: Pubkey,
    recent_blockhash: Hash,

    entry_buffer: Vec<u8>,     // Current batch being built
    entry_count: u64,          // Num entries in current batch
    batch_tick_reference: u8,  // Tick reference of current batch
    target_batch_entries: u64, // Num entries targeting for this batch

    current_slot: u64,
    current_slot_start_time: Instant,
    current_code_shred_index: u32,
    current_data_shred_index: u32,
    entries_in_slot: u32,
    transactions_in_slot: u32,
    transactions_drift_in_slot: u32,
    is_slot_ending: bool,

    total_data_shreds: u32,
    total_code_shreds: u32,
    total_transactions: u32,
    total_transactions_drift: u32,

    reorder_probability: f32,
    shred_send_queue: Arc<Mutex<VecDeque<Vec<u8>>>>,
    current_fec_set_data_shreds: Vec<Vec<u8>>,
}

impl MockValidator {
    pub fn new(target: String) -> Result<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.set_nonblocking(true)?;

        let drift_program_id = Pubkey::from_str("dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH")?;

        let mut payer_keypairs = Vec::new();
        let mut user_keypairs = Vec::new();
        for _ in 0..100 {
            payer_keypairs.push(Keypair::new());
            user_keypairs.push(Keypair::new());
        }

        Ok(Self {
            start_time: Instant::now(),
            socket,
            target,
            shred_version: 50093,
            payer_keypairs,
            user_keypairs,
            keypair_index: AtomicUsize::new(0),
            drift_program_id,
            recent_blockhash: Hash::new_unique(),
            entry_buffer: Vec::new(),
            entry_count: 0,
            batch_tick_reference: 0,
            target_batch_entries: 0,
            current_slot: 100,
            current_slot_start_time: Instant::now(),
            current_code_shred_index: 0,
            current_data_shred_index: 0,
            entries_in_slot: 0,
            transactions_in_slot: 0,
            transactions_drift_in_slot: 0,
            total_data_shreds: 0,
            total_code_shreds: 0,
            total_transactions: 0,
            total_transactions_drift: 0,
            is_slot_ending: false,
            reorder_probability: 0.05,
            shred_send_queue: Arc::new(Mutex::new(VecDeque::new())),
            current_fec_set_data_shreds: Vec::new(),
        })
    }

    pub fn run(&mut self, shreds_per_second: u32) -> Result<()> {
        // Sender thread
        let shred_queue = Arc::clone(&self.shred_send_queue);
        let socket = self.socket.try_clone()?;
        let target = self.target.clone();
        let reorder_prob = self.reorder_probability;
        std::thread::spawn(move || {
            Self::sender_thread(shred_queue, socket, target, shreds_per_second, reorder_prob)
        });

        let mut rng = rand::rng();
        let mut last_slot_advance = Instant::now();

        loop {
            let queue_len = self.shred_send_queue.lock().unwrap().len();

            // Generate entries for this slot
            if queue_len < (shreds_per_second * 2) as usize {
                self.generate_entries(&mut rng)?;
            }

            // Check if we should end current slot
            if last_slot_advance.elapsed() > Duration::from_millis(SLOT_DURATION_MS) {
                self.finalize_current_slot(&mut rng)?;
                last_slot_advance = Instant::now();
            }

            std::thread::sleep(Duration::from_micros(500));
        }
    }

    fn sender_thread(
        shred_queue: Arc<Mutex<VecDeque<Vec<u8>>>>,
        socket: UdpSocket,
        target: String,
        shreds_per_second: u32,
        reorder_probability: f32,
    ) {
        let mut reorder_buffer: VecDeque<(Vec<u8>, u8)> = VecDeque::new();
        let mut rng = rand::rng();
        let shred_interval = Duration::from_nanos(1_000_000_000 / shreds_per_second as u64);
        let mut next_send_time = Instant::now();
        let mut reorder_tick = 0u32;
        let num_loops_batch = 10u8;

        loop {
            let now = Instant::now();

            // Send reordered shreds
            reorder_tick += 1;
            if reorder_tick % num_loops_batch as u32 == 0 {
                let mut i = 0;
                while i < reorder_buffer.len() {
                    reorder_buffer[i].1 = reorder_buffer[i].1.saturating_sub(num_loops_batch);
                    if reorder_buffer[i].1 == 0 {
                        let (shred_data, _) = reorder_buffer.remove(i).unwrap();
                        if let Err(e) = socket.send_to(&shred_data, &target) {
                            warn!("Failed to send reordered packet: {}", e);
                        }
                    } else {
                        i += 1;
                    }
                }
            }

            // Send or reorder next packet
            if now >= next_send_time {
                let shreds_batch: Vec<Vec<u8>> = {
                    let mut batch = Vec::new();
                    let mut queue = shred_queue.lock().unwrap();
                    for _ in 0..num_loops_batch {
                        if let Some(shred) = queue.pop_front() {
                            batch.push(shred);
                        } else {
                            break;
                        }
                    }
                    batch
                };

                for shred in shreds_batch {
                    if rng.random::<f32>() < reorder_probability {
                        let delay = rng.random_range(50..=200);
                        reorder_buffer.push_back((shred, delay));
                    } else {
                        if let Err(e) = socket.send_to(&shred, &target) {
                            warn!("Failed to send packet: {}", e);
                        }
                    }
                    next_send_time += shred_interval;
                }
            }
        }
    }

    /// Create batch of varying number of entries each with varying number of transactions
    fn generate_entries(&mut self, rng: &mut impl Rng) -> Result<()> {
        // Start new batch if buffer is empty
        if self.entry_buffer.is_empty() {
            self.target_batch_entries = rng
                .random_range(MIN_TARGET_ENTRIES_PER_BATCH..=MAX_TARGET_ENTRIES_PER_BATCH)
                as u64;

            let elapsed = self.current_slot_start_time.elapsed().as_micros() as u64;
            self.batch_tick_reference =
                ((elapsed * TICKS_PER_SLOT) / (SLOT_DURATION_MS * 1000)) as u8;

            self.entry_buffer
                .extend_from_slice(&self.target_batch_entries.to_le_bytes());
            self.entry_count = 0;
        }

        while self.entry_count < self.target_batch_entries {
            let num_txs = rng.random_range(MIN_TARGET_TXS_PER_ENTRY..=MAX_TARGET_TXS_PER_ENTRY);
            let mut transactions = Vec::new();

            for _ in 0..num_txs {
                if rng.random_bool(0.01) {
                    // % of drift txs
                    transactions.push(self.create_mock_drift_transaction());
                    self.transactions_drift_in_slot += 1;
                } else {
                    transactions.push(self.create_mock_transfer_transaction());
                }
            }

            let entry = Entry {
                num_hashes: rng.random_range(1..4),
                hash: Hash::new_unique(),
                transactions,
            };

            let serialized = bincode::serialize(&entry)?;
            self.entry_buffer.extend_from_slice(&serialized);
            self.entry_count += 1;
            self.entries_in_slot += 1;
            self.transactions_in_slot += num_txs as u32;
        }

        self.flush_entry_buffer()?;

        Ok(())
    }

    fn flush_entry_buffer(&mut self) -> Result<()> {
        while !self.entry_buffer.is_empty() {
            let chunk_size = self.entry_buffer.len().min(MERKLE_DATA_CAPACITY);
            let payload: Vec<u8> = self.entry_buffer.drain(..chunk_size).collect();

            let is_batch_complete = self.entry_buffer.is_empty();
            let is_block_complete = is_batch_complete && self.is_slot_ending;

            let fec_set_index = (self.current_data_shred_index / FEC_SET_SIZE) * FEC_SET_SIZE;

            let shred = Self::create_data_shred(
                self.current_slot,
                self.shred_version,
                self.current_data_shred_index,
                1,
                &payload,
                is_batch_complete,
                is_block_complete,
                self.batch_tick_reference,
                fec_set_index,
            );

            // Add to FEC set
            self.current_fec_set_data_shreds.push(shred.clone());
            {
                let mut queue = self.shred_send_queue.lock().unwrap();
                queue.push_back(shred);
            }
            self.current_data_shred_index += 1;

            // If FEC complete, generate coding shreds
            if self.current_fec_set_data_shreds.len() == DATA_SHREDS_PER_FEC_BLOCK
                || (is_block_complete && !self.current_fec_set_data_shreds.is_empty())
            {
                self.generate_and_queue_mock_code_shreds(fec_set_index)?;
            }
        }

        Ok(())
    }

    fn generate_and_queue_mock_code_shreds(&mut self, fec_set_index: u32) -> Result<()> {
        let num_data_shreds = self.current_fec_set_data_shreds.len() as u16;
        let num_coding_shreds = Self::get_num_coding_shreds(num_data_shreds);

        let mut queue = self.shred_send_queue.lock().unwrap();
        for i in 0..num_coding_shreds {
            let code_shred = Self::create_mock_code_shred(
                self.current_slot,
                self.shred_version,
                self.current_code_shred_index + i as u32,
                fec_set_index,
                num_data_shreds,
                num_coding_shreds,
                i,
            );
            queue.push_back(code_shred);
        }

        self.current_code_shred_index += num_coding_shreds as u32;
        self.current_fec_set_data_shreds.clear();
        Ok(())
    }

    fn create_data_shred(
        slot: u64,
        shred_version: u16,
        index: u32,
        parent_offset: u16,
        payload: &[u8],
        is_batch_complete: bool,
        is_block_complete: bool,
        tick_reference: u8,
        fec_set_index: u32,
    ) -> Vec<u8> {
        let mut shred = vec![0u8; MERKLE_DATA_SHRED_SIZE];

        // Copy payload
        shred[DATA_OFFSET_PAYLOAD..DATA_OFFSET_PAYLOAD + payload.len()].copy_from_slice(payload);

        // Common header
        shred[OFFSET_SHRED_VARIANT] = MERKLE_DATA_VARIANT | PROOF_HEIGHT;
        shred[OFFSET_SHRED_SLOT..OFFSET_SHRED_SLOT + 8].copy_from_slice(&slot.to_le_bytes());
        shred[OFFSET_SHRED_INDEX..OFFSET_SHRED_INDEX + 4].copy_from_slice(&index.to_le_bytes());
        shred[OFFSET_SHRED_VERSION..OFFSET_SHRED_VERSION + 2]
            .copy_from_slice(&shred_version.to_le_bytes());
        shred[OFFSET_FEC_SET_INDEX..OFFSET_FEC_SET_INDEX + 4]
            .copy_from_slice(&fec_set_index.to_le_bytes());

        // Data header
        shred[OFFSET_PARENT_OFFSET..OFFSET_PARENT_OFFSET + 2]
            .copy_from_slice(&parent_offset.to_le_bytes());

        let mut flags = tick_reference & 0b0011_1111; // Add in batch tick number
        if is_batch_complete {
            flags |= 0b0100_0000; // Set batch_complete
        }
        if is_block_complete {
            flags |= 0b1000_0000; // Set block_complete
        }
        shred[OFFSET_FLAGS] = flags;

        let total_size = (DATA_OFFSET_PAYLOAD + payload.len()) as u16;
        shred[OFFSET_SIZE..OFFSET_SIZE + 2].copy_from_slice(&total_size.to_le_bytes());

        // Sign erasure-coded portion
        // let erasure_start = SIGNATURE_BYTES;
        let erasure_end = MERKLE_DATA_SHRED_SIZE - MERKLE_PROOF_SIZE;
        // let signature = leader_keypair.sign_message(&shred[erasure_start..erasure_end]);
        shred[..SIGNATURE_BYTES].fill(0xCC);

        // Mock merkle proof
        shred[erasure_end..].fill(0xAA);

        shred
    }

    fn create_mock_drift_transaction(&self) -> VersionedTransaction {
        let idx = self.keypair_index.fetch_add(1, Ordering::Relaxed);
        let payer = &self.payer_keypairs[idx % self.payer_keypairs.len()];
        let user = &self.user_keypairs[idx % self.user_keypairs.len()];

        let discriminator_idx = idx % DRIFT_LIQUIDATION_DISCRIMINATORS.len();
        let (discriminator, _liquidation_type) =
            DRIFT_LIQUIDATION_DISCRIMINATORS[discriminator_idx];
        let mut instruction_data = discriminator.to_vec();
        instruction_data.extend_from_slice(&[0u8; 24]);

        let instruction = Instruction {
            program_id: self.drift_program_id,
            accounts: vec![
                AccountMeta::new(user.pubkey(), true),
                AccountMeta::new_readonly(payer.pubkey(), true),
            ],
            data: instruction_data,
        };

        let message = Message::new(&[instruction], Some(&payer.pubkey()));
        let tx = Transaction::new(&[&payer, &user], message, self.recent_blockhash);
        VersionedTransaction::from(tx)
    }

    fn create_mock_transfer_transaction(&self) -> VersionedTransaction {
        let idx = self.keypair_index.fetch_add(1, Ordering::Relaxed);
        let from = &self.payer_keypairs[idx % self.payer_keypairs.len()];
        let to = &self.user_keypairs[idx % self.user_keypairs.len()];

        let instruction =
            solana_system_interface::instruction::transfer(&from.pubkey(), &to.pubkey(), 1_234_567);

        let message = Message::new(&[instruction], Some(&from.pubkey()));
        let tx = Transaction::new(&[&from], message, self.recent_blockhash);
        VersionedTransaction::from(tx)
    }

    fn finalize_current_slot(&mut self, rng: &mut impl Rng) -> Result<()> {
        self.is_slot_ending = true;
        // Create final batch with remaining entries
        if !self.entry_buffer.is_empty() {
            self.flush_entry_buffer()?;
        } else {
            // Need to generate is_block_complete entry batch
            self.generate_entries(rng)?;
        }

        self.advance_slot();
        Ok(())
    }

    fn create_mock_code_shred(
        slot: u64,
        shred_version: u16,
        index: u32,
        fec_set_index: u32,
        num_data_shreds: u16,
        num_coding_shreds: u16,
        position: u16,
    ) -> Vec<u8> {
        let mut shred = vec![0u8; MERKLE_CODE_SHRED_SIZE];

        // Common header
        shred[OFFSET_SHRED_VARIANT] = MERKLE_CODE_VARIANT | PROOF_HEIGHT;
        shred[OFFSET_SHRED_SLOT..OFFSET_SHRED_SLOT + 8].copy_from_slice(&slot.to_le_bytes());
        shred[OFFSET_SHRED_INDEX..OFFSET_SHRED_INDEX + 4].copy_from_slice(&index.to_le_bytes());
        shred[OFFSET_SHRED_VERSION..OFFSET_SHRED_VERSION + 2]
            .copy_from_slice(&shred_version.to_le_bytes());
        shred[OFFSET_FEC_SET_INDEX..OFFSET_FEC_SET_INDEX + 4]
            .copy_from_slice(&fec_set_index.to_le_bytes());

        // Code header
        shred[0x53..0x55].copy_from_slice(&num_data_shreds.to_le_bytes());
        shred[0x55..0x57].copy_from_slice(&num_coding_shreds.to_le_bytes());
        shred[0x57..0x59].copy_from_slice(&position.to_le_bytes());

        // Mock signature
        shred[..SIGNATURE_BYTES].fill(0xDD);

        // Mock payload
        let parity_start = CODE_OFFSET_PAYLOAD;
        let parity_end = MERKLE_CODE_SHRED_SIZE - MERKLE_PROOF_SIZE;
        shred[parity_start..parity_end].fill(0xEE);

        // Mock merkle proof
        let proof_offset = MERKLE_CODE_SHRED_SIZE - MERKLE_PROOF_SIZE;
        shred[proof_offset..].fill(0xBB);

        shred
    }

    fn advance_slot(&mut self) {
        info!(
            "Completed slot {} (created {} data shreds, {} code shreds, {} entries, {} transactions, {} drift tx)",
            self.current_slot,
            self.current_data_shred_index,
            self.current_code_shred_index,
            self.entries_in_slot,
            self.transactions_in_slot,
            self.transactions_drift_in_slot
        );

        self.total_data_shreds += self.current_data_shred_index;
        self.total_code_shreds += self.current_code_shred_index;
        self.total_transactions += self.transactions_in_slot;
        self.total_transactions_drift += self.transactions_drift_in_slot;
        let elapsed_secs = self.start_time.elapsed().as_secs_f64();
        let total_shreds = self.total_data_shreds + self.total_code_shreds;
        let shreds_per_sec = if elapsed_secs > 0.0 {
            total_shreds as f64 / elapsed_secs
        } else {
            0.0
        };

        info!(
            "Total created: {} data shreds, {} code shreds, {} transactions, {} drift tx,  {:.1} shreds/sec)",
            self.total_data_shreds,
            self.total_code_shreds,
            self.total_transactions,
            self.total_transactions_drift,
            shreds_per_sec,
        );

        self.current_slot += 1;
        self.current_data_shred_index = 0;
        self.current_code_shred_index = 0;
        self.transactions_in_slot = 0;
        self.transactions_drift_in_slot = 0;
        self.current_slot_start_time = Instant::now();
        self.recent_blockhash = Hash::new_unique();
        self.entries_in_slot = 0;
        self.is_slot_ending = false;

        self.entry_buffer.clear();
        self.entry_count = 0;
        self.batch_tick_reference = 0;
        self.target_batch_entries = 0;
    }

    fn get_num_coding_shreds(num_data_shreds: u16) -> u16 {
        match num_data_shreds {
            1 => 17,
            2 => 18,
            3 => 19,
            4 => 19,
            5 => 20,
            6 => 21,
            7 => 21,
            8 => 22,
            9 => 23,
            10 => 23,
            11 => 24,
            12 => 24,
            13 => 25,
            14 => 25,
            15 => 26,
            16 => 26,
            17 => 26,
            18 => 27,
            19 => 27,
            20 => 28,
            21 => 28,
            22 => 29,
            23 => 29,
            24 => 29,
            25 => 30,
            26 => 30,
            27 => 31,
            28 => 31,
            29 => 31,
            30 => 32,
            31 => 32,
            32 => 32,
            n if n > 32 => n,
            _ => panic!("Invalid number of data shreds: {}", num_data_shreds),
        }
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let target = std::env::var("TARGET_ADDR").unwrap_or_else(|_| "127.0.0.1:8001".to_string());
    let shreds_per_second: u32 = std::env::var("SHREDS_PER_SECOND")
        .unwrap_or_else(|_| 10000u32.to_string())
        .parse()?;

    info!(
        "Starting mock validator sending {} shreds/sec to {}",
        shreds_per_second, target
    );

    let mut validator = MockValidator::new(target)?;
    validator.run(shreds_per_second)
}
