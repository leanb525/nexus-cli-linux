// Copyright (c) 2024 Nexus. All rights reserved.

// Use jemalloc allocator to solve memory fragmentation issues
#[cfg(feature = "jemalloc")]
use jemallocator::Jemalloc;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use clap::{Parser, Subcommand};
use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    Terminal,
};
use std::error::Error;
use std::path::PathBuf;
use std::io::{self, Write};
use std::fmt::Write as FmtWrite;
use tokio::task::JoinSet;
use std::sync::Arc;
use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::sync::{broadcast, Semaphore};
use smallvec::SmallVec;
use compact_str::CompactString;
use lru::LruCache;
use std::num::NonZeroUsize;
use rayon::prelude::*;

// 新增: HTTP头部随机化相关导入
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT, ACCEPT, ACCEPT_LANGUAGE, ACCEPT_ENCODING};
use rand::Rng;
use once_cell::sync::Lazy;

mod analytics;
mod config;
mod environment;
mod keys;
#[path = "proto/nexus.orchestrator.rs"]
mod nexus_orchestrator;
mod orchestrator_client;
mod prover;
mod prover_runtime;
mod setup;
mod task;
mod ui;
mod utils;
mod node_list;

use crate::config::Config;
use crate::environment::Environment;
use crate::setup::clear_node_config;
use crate::node_list::NodeList;

// ========== HTTP头部随机化功能 ==========

// 全球家宽IP段（主要ISP的住宅网络）
static RESIDENTIAL_IP_RANGES: Lazy<Vec<&'static str>> = Lazy::new(|| vec![
    // 美国主要ISP住宅网段
    "24.0.0.0/8", "76.0.0.0/8", "98.0.0.0/8", "174.0.0.0/8",
    "108.0.0.0/8", "173.0.0.0/8", "67.0.0.0/8", "75.0.0.0/8",
    
    // 加拿大住宅网段
    "99.0.0.0/8", "142.0.0.0/8", "184.0.0.0/8", "70.0.0.0/8",
    
    // 英国住宅网段  
    "86.0.0.0/8", "90.0.0.0/8", "109.0.0.0/8", "82.0.0.0/8",
    "81.0.0.0/8", "87.0.0.0/8", "92.0.0.0/8", "94.0.0.0/8",
    
    // 德国住宅网段
    "91.0.0.0/8", "93.0.0.0/8", "95.0.0.0/8", "84.0.0.0/8",
    "85.0.0.0/8", "89.0.0.0/8", "88.0.0.0/8",
    
    // 法国住宅网段
    "83.0.0.0/8", "78.0.0.0/8", "79.0.0.0/8", "80.0.0.0/8",
    "77.0.0.0/8", "86.0.0.0/8", "90.0.0.0/8",
    
    // 日本住宅网段
    "126.0.0.0/8", "133.0.0.0/8", "210.0.0.0/8", "219.0.0.0/8",
    "220.0.0.0/8", "221.0.0.0/8", "222.0.0.0/8",
    
    // 韩国住宅网段
    "175.0.0.0/8", "211.0.0.0/8", "218.0.0.0/8", "112.0.0.0/8",
    
    // 澳大利亚住宅网段
    "101.0.0.0/8", "103.0.0.0/8", "110.0.0.0/8", "114.0.0.0/8",
    "115.0.0.0/8", "116.0.0.0/8", "117.0.0.0/8", "118.0.0.0/8",
    
    // 新西兰住宅网段
    "49.0.0.0/8", "122.0.0.0/8", "124.0.0.0/8", "125.0.0.0/8",
    
    // 荷兰住宅网段
    "84.0.0.0/8", "85.0.0.0/8", "145.0.0.0/8", "213.0.0.0/8",
    
    // 瑞典住宅网段
    "78.0.0.0/8", "81.0.0.0/8", "83.0.0.0/8", "90.0.0.0/8",
    
    // 挪威住宅网段
    "84.0.0.0/8", "85.0.0.0/8", "129.0.0.0/8", "158.0.0.0/8",
    
    // 意大利住宅网段
    "79.0.0.0/8", "87.0.0.0/8", "93.0.0.0/8", "95.0.0.0/8",
    
    // 西班牙住宅网段
    "83.0.0.0/8", "88.0.0.0/8", "90.0.0.0/8", "95.0.0.0/8",
    
    // 巴西住宅网段
    "177.0.0.0/8", "179.0.0.0/8", "186.0.0.0/8", "189.0.0.0/8",
    
    // 印度住宅网段
    "117.0.0.0/8", "122.0.0.0/8", "157.0.0.0/8", "182.0.0.0/8",
]);

// 真实的浏览器User-Agent
static USER_AGENTS: Lazy<Vec<&'static str>> = Lazy::new(|| vec![
    // Chrome Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    
    // Chrome macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    
    // Firefox Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    
    // Firefox macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.6; rv:120.0) Gecko/20100101 Firefox/120.0",
    
    // Safari macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    
    // Edge Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
    
    // Chrome Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
]);

// 语言偏好
static ACCEPT_LANGUAGES: Lazy<Vec<&'static str>> = Lazy::new(|| vec![
    "en-US,en;q=0.9",
    "en-GB,en;q=0.9",
    "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
    "en-US,en;q=0.9,fr;q=0.8",
    "en-US,en;q=0.9,de;q=0.8",
    "en-US,en;q=0.9,ja;q=0.8",
    "en-US,en;q=0.9,es;q=0.8",
    "en-CA,en;q=0.9,fr;q=0.8",
    "en-AU,en;q=0.9",
    "en-NZ,en;q=0.9",
]);

fn generate_residential_ip() -> String {
    let mut rng = rand::thread_rng();
    let range = RESIDENTIAL_IP_RANGES[rng.gen_range(0..RESIDENTIAL_IP_RANGES.len())];
    
    // 解析CIDR并生成随机IP
    let parts: Vec<&str> = range.split('/').collect();
    let base_ip = parts[0];
    let _prefix_len: u8 = parts[1].parse().unwrap_or(8);
    
    let ip_parts: Vec<&str> = base_ip.split('.').collect();
    let base_octet: u8 = ip_parts[0].parse().unwrap_or(192);
    
    // 为了更真实，在同一个/8网段内生成随机IP
    format!("{}.{}.{}.{}", 
        base_octet,
        rng.gen_range(1..255),
        rng.gen_range(1..255),
        rng.gen_range(1..254)
    )
}

pub fn create_randomized_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    let mut rng = rand::thread_rng();
    
    // 随机User-Agent
    let ua = USER_AGENTS[rng.gen_range(0..USER_AGENTS.len())];
    headers.insert(USER_AGENT, HeaderValue::from_str(ua).unwrap());
    
    // 生成随机住宅IP
    let fake_ip = generate_residential_ip();
    headers.insert("X-Forwarded-For", HeaderValue::from_str(&fake_ip).unwrap());
    headers.insert("X-Real-IP", HeaderValue::from_str(&fake_ip).unwrap());
    headers.insert("X-Client-IP", HeaderValue::from_str(&fake_ip).unwrap());
    headers.insert("X-Remote-IP", HeaderValue::from_str(&fake_ip).unwrap());
    headers.insert("X-Originating-IP", HeaderValue::from_str(&fake_ip).unwrap());
    
    // 随机Accept-Language
    let lang = ACCEPT_LANGUAGES[rng.gen_range(0..ACCEPT_LANGUAGES.len())];
    headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_str(lang).unwrap());
    
    // 标准头部
    headers.insert(ACCEPT, HeaderValue::from_static("application/json,text/plain,*/*"));
    headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip, deflate, br"));
    headers.insert("Cache-Control", HeaderValue::from_static("no-cache"));
    headers.insert("Pragma", HeaderValue::from_static("no-cache"));
    
    // 随机连接信息
    let connection_types = ["keep-alive", "close"];
    let conn = connection_types[rng.gen_range(0..connection_types.len())];
    headers.insert("Connection", HeaderValue::from_str(conn).unwrap());
    
    // 随机DNT
    if rng.gen_bool(0.7) {
        headers.insert("DNT", HeaderValue::from_static("1"));
    }
    
    // 随机Sec-Fetch headers (模拟真实浏览器)
    headers.insert("Sec-Fetch-Dest", HeaderValue::from_static("empty"));
    headers.insert("Sec-Fetch-Mode", HeaderValue::from_static("cors"));
    headers.insert("Sec-Fetch-Site", HeaderValue::from_static("same-origin"));
    
    headers
}

pub fn create_http_client() -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .default_headers(create_randomized_headers())
        .timeout(std::time::Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::limited(3))
        .build()
}

// ========== 原有代码继续 ==========

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
/// Command-line arguments
struct Args {
    /// Command to execute
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the prover
    Start {
        /// Node ID
        #[arg(long, value_name = "NODE_ID")]
        node_id: Option<u64>,
        /// Environment to connect to.
        #[arg(long, value_enum)]
        env: Option<Environment>,
    },
    
    /// Start multiple provers from node list file
    BatchFile {
        /// Path to node list file (.txt)
        #[arg(long, value_name = "FILE_PATH")]
        file: String,
        /// Environment to connect to.
        #[arg(long, value_enum)]
        env: Option<Environment>,
        /// Delay between starting each node (seconds)
        #[arg(long, default_value = "10")]  // Changed from 0.5 to 10
        start_delay: f64,
        /// Delay between proof submissions per node (seconds)
        #[arg(long, default_value = "1")]
        proof_interval: u64,
        /// Maximum number of concurrent nodes
        #[arg(long, default_value = "10")]
        max_concurrent: usize,
        /// Enable verbose error logging
        #[arg(long)]
        verbose: bool,
    },
    /// Create example node list files
    CreateExamples {
        /// Directory to create example files
        #[arg(long, default_value = "./examples")]
        dir: String,
    },
    
    /// Logout from the current session
    Logout,
}

/// Get the path to the Nexus config file, typically located at ~/.nexus/config.json.
fn get_config_path() -> Result<PathBuf, ()> {
    let home_path = home::home_dir().expect("Failed to get home directory");
    let config_path = home_path.join(".nexus").join("config.json");
    Ok(config_path)
}

/// Memory tracking for performance monitoring
#[derive(Debug)]
struct MemoryTracker {
    #[allow(dead_code)]
    initial_rss: AtomicU64,
    peak_rss: AtomicU64,
    #[allow(dead_code)]
    allocations: AtomicU64,
}

impl MemoryTracker {
    fn new() -> Self {
        let initial_rss = Self::get_current_rss();
        Self {
            initial_rss: AtomicU64::new(initial_rss),
            peak_rss: AtomicU64::new(initial_rss),
            allocations: AtomicU64::new(0),
        }
    }
    
    #[cfg(target_os = "linux")]
    fn get_current_rss() -> u64 {
        use std::fs;
        fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|content| {
                content.lines()
                    .find(|line| line.starts_with("VmRSS:"))
                    .and_then(|line| {
                        line.split_whitespace()
                            .nth(1)
                            .and_then(|s| s.parse::<u64>().ok())
                    })
            })
            .unwrap_or(0) * 1024 // Convert KB to bytes
    }
    
    #[cfg(not(target_os = "linux"))]
    fn get_current_rss() -> u64 {
        0 // Fallback for non-Linux systems
    }
    
    fn update_peak(&self) {
        let current = Self::get_current_rss();
        let mut peak = self.peak_rss.load(Ordering::Relaxed);
        while current > peak {
            match self.peak_rss.compare_exchange_weak(
                peak, 
                current, 
                Ordering::Relaxed, 
                Ordering::Relaxed
            ) {
                Ok(_) => break,
                Err(new_peak) => peak = new_peak,
            }
        }
    }
}

/// Optimized string pool for reducing allocations
struct StringPool {
    pool: Arc<tokio::sync::Mutex<Vec<String>>>,
    capacity: usize,
}

impl std::fmt::Debug for StringPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StringPool")
            .field("capacity", &self.capacity)
            .finish()
    }
}

impl StringPool {
    fn new(capacity: usize) -> Self {
        Self {
            pool: Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(capacity))),
            capacity,
        }
    }
    
    async fn get_string(&self) -> String {
        let mut pool = self.pool.lock().await;
        pool.pop().unwrap_or_else(|| String::with_capacity(256))
    }
    
    async fn return_string(&self, mut string: String) {
        string.clear();
        let mut pool = self.pool.lock().await;
        if pool.len() < self.capacity {
            pool.push(string);
        }
    }
}

/// Smart status cache with LRU eviction
struct StatusCache {
    #[allow(dead_code)]
    cache: tokio::sync::Mutex<LruCache<u64, CompactString>>,
    hit_count: AtomicU64,
    miss_count: AtomicU64,
}

impl StatusCache {
    fn new(capacity: usize) -> Self {
        Self {
            cache: tokio::sync::Mutex::new(
                LruCache::new(NonZeroUsize::new(capacity).unwrap())
            ),
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
        }
    }
    
    #[allow(dead_code)]
    async fn get_or_insert<F>(&self, key: u64, f: F) -> CompactString
    where
        F: FnOnce() -> CompactString,
    {
        let mut cache = self.cache.lock().await;
        match cache.get(&key) {
            Some(value) => {
                self.hit_count.fetch_add(1, Ordering::Relaxed);
                value.clone()
            }
            None => {
                self.miss_count.fetch_add(1, Ordering::Relaxed);
                let value = f();
                cache.put(key, value.clone());
                value
            }
        }
    }
    
    fn get_hit_rate(&self) -> f64 {
        let hits = self.hit_count.load(Ordering::Relaxed);
        let misses = self.miss_count.load(Ordering::Relaxed);
        if hits + misses == 0 {
            0.0
        } else {
            hits as f64 / (hits + misses) as f64
        }
    }
}

impl std::fmt::Debug for StatusCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatusCache")
            .field("hit_count", &self.hit_count.load(Ordering::Relaxed))
            .field("miss_count", &self.miss_count.load(Ordering::Relaxed))
            .finish()
    }
}

/// Optimized task pool with semaphore-based concurrency control
struct TaskPool {
    #[allow(dead_code)]
    semaphore: Arc<Semaphore>,
    active_tasks: AtomicUsize,
    completed_tasks: AtomicU64,
    failed_tasks: AtomicU64,
}

impl std::fmt::Debug for TaskPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskPool")
            .field("active_tasks", &self.active_tasks.load(Ordering::Relaxed))
            .field("completed_tasks", &self.completed_tasks.load(Ordering::Relaxed))
            .field("failed_tasks", &self.failed_tasks.load(Ordering::Relaxed))
            .finish()
    }
}

impl TaskPool {
    fn new(max_concurrent: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            active_tasks: AtomicUsize::new(0),
            completed_tasks: AtomicU64::new(0),
            failed_tasks: AtomicU64::new(0),
        }
    }
    
    fn get_stats(&self) -> (usize, u64, u64) {
        (
            self.active_tasks.load(Ordering::Relaxed),
            self.completed_tasks.load(Ordering::Relaxed),
            self.failed_tasks.load(Ordering::Relaxed),
        )
    }
}

/// Optimized fixed line display manager with advanced memory management
#[derive(Debug)]
struct FixedLineDisplay {
    #[allow(dead_code)]
    max_lines: usize,
    node_lines: Arc<tokio::sync::RwLock<HashMap<u64, CompactString>>>,
    last_render_hash: Arc<tokio::sync::Mutex<u64>>,
    defragmenter: Arc<crate::utils::system::MemoryDefragmenter>,
    #[allow(dead_code)]
    string_pool: Arc<StringPool>,
    #[allow(dead_code)]
    status_cache: Arc<StatusCache>,
    memory_tracker: Arc<MemoryTracker>,
}

impl FixedLineDisplay {
    fn new(max_lines: usize) -> Self {
        Self {
            max_lines,
            node_lines: Arc::new(tokio::sync::RwLock::new(
                HashMap::with_capacity(max_lines)
            )),
            last_render_hash: Arc::new(tokio::sync::Mutex::new(0)),
            defragmenter: Arc::new(crate::utils::system::MemoryDefragmenter::new()),
            string_pool: Arc::new(StringPool::new(max_lines * 2)),
            status_cache: Arc::new(StatusCache::new(max_lines * 4)),
            memory_tracker: Arc::new(MemoryTracker::new()),
        }
    }

    async fn update_node_status(&self, node_id: u64, status: String) {
        let compact_status = CompactString::new(&status);
        
        let needs_update = {
            let lines = self.node_lines.read().await;
            lines.get(&node_id) != Some(&compact_status)
        };

        if needs_update {
            {
                let mut lines = self.node_lines.write().await;
                lines.insert(node_id, compact_status);
            }
            self.render_display_optimized().await;
        }
    }

    #[allow(dead_code)]
    async fn remove_node(&self, node_id: u64) {
        {
            let mut lines = self.node_lines.write().await;
            lines.remove(&node_id);
        }
        self.render_display_optimized().await;
    }

    async fn render_display_optimized(&self) {
        self.memory_tracker.update_peak();
        
        let lines = self.node_lines.read().await;

        if self.defragmenter.should_defragment().await {
            let result = self.defragmenter.defragment().await;
            
            if result.was_critical {
                println!("Critical memory cleanup complete:");
            } else {
                println!("Regular memory cleanup complete:");
            }
            println!("   Memory: {:.1}% -> {:.1}% (freed {:.1}%)", 
                     result.memory_before * 100.0, 
                     result.memory_after * 100.0,
                     result.memory_freed_percentage());
            println!("   Freed space: {} KB", result.bytes_freed / 1024);
        }

        if crate::utils::system::check_memory_pressure() {
            println!("High memory usage detected, performing additional cleanup...");
            crate::utils::system::perform_memory_cleanup();
        }

        let current_hash = if lines.len() > 100 {
            lines.par_iter()
                .map(|(id, status)| {
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    hasher.write_u64(*id);
                    hasher.write(status.as_bytes());
                    hasher.finish()
                })
                .reduce(|| 0, |a, b| a ^ b)
        } else {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            for (id, status) in lines.iter() {
                hasher.write_u64(*id);
                hasher.write(status.as_bytes());
            }
            hasher.finish()
        };

        let mut last_hash = self.last_render_hash.lock().await;
        if *last_hash != current_hash {
            *last_hash = current_hash;
            drop(last_hash);
            self.render_display(&lines).await;
        }
    }

    async fn render_display(&self, lines: &HashMap<u64, CompactString>) {
        print!("\x1b[2J\x1b[H");

        let time_buffer = format!("{}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S"));

        println!("Nexus Enhanced Batch Mining Monitor - {}", time_buffer);
        println!("=======================================");

        let (total_nodes, successful_count, failed_count, active_count) = if lines.len() > 50 {
            lines.par_iter()
                .map(|(_, status)| {
                    let status_str = status.as_str();
                    (
                        1,
                        if status_str.contains("Success") || status_str.contains("OK") { 1 } else { 0 },
                        if status_str.contains("Error") || status_str.contains("Failed") { 1 } else { 0 },
                        if status_str.contains("Running") || status_str.contains("Active") || status_str.contains("Task Fetcher") { 1 } else { 0 },
                    )
                })
                .reduce(|| (0, 0, 0, 0), |a, b| (a.0 + b.0, a.1 + b.1, a.2 + b.2, a.3 + b.3))
        } else {
            lines.iter().fold((0, 0, 0, 0), |(total, success, failed, active), (_, status)| {
                let status_str = status.as_str();
                (
                    total + 1,
                    success + if status_str.contains("Success") || status_str.contains("OK") { 1 } else { 0 },
                    failed + if status_str.contains("Error") || status_str.contains("Failed") { 1 } else { 0 },
                    active + if status_str.contains("Running") || status_str.contains("Active") || status_str.contains("Task Fetcher") { 1 } else { 0 },
                )
            })
        };

        println!("Status: {} Total | {} Active | {} Success | {} Failed", 
                 total_nodes, active_count, successful_count, failed_count);

        let stats = self.defragmenter.get_stats().await;
        let cache_hit_rate = self.status_cache.get_hit_rate() * 100.0;
        
        if stats.total_checks > 0 {
            println!("Memory: {:.1}% | Cache: {:.1}% hit | Cleanups: {} | Freed: {} KB", 
                     crate::utils::system::get_memory_usage_ratio() * 100.0,
                     cache_hit_rate,
                     stats.cleanups_performed,
                     stats.bytes_freed / 1024);
        } else {
            let (used_mb, total_mb) = crate::utils::system::get_memory_info();
            let usage_percentage = (used_mb as f64 / total_mb as f64) * 100.0;
            println!("Memory: {:.1}% ({} MB / {} MB) | Cache: {:.1}%", 
                     usage_percentage, used_mb, total_mb, cache_hit_rate);
        }

        println!("---------------------------------------");

        let mut sorted_lines: SmallVec<[_; 32]> = SmallVec::with_capacity(lines.len());
        sorted_lines.extend(lines.iter());
        sorted_lines.sort_unstable_by_key(|(id, _)| *id);

        for (node_id, status) in sorted_lines.iter() {
            println!("Node-{}: {}", node_id, status);
        }

        println!("---------------------------------------");
        println!("Press Ctrl+C to stop all miners");
        println!("HTTP Headers: Randomized with residential IPs");  // 新增提示

        let _ = io::stdout().flush();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .format_timestamp_secs()
        .init();

    let args = Args::parse();
    match args.command {
        Command::Start { node_id, env } => {
            let mut node_id = node_id;
            let config_path = get_config_path().expect("Failed to get config path");
            if node_id.is_none() && config_path.exists() {
                if let Ok(config) = Config::load_from_file(&config_path) {
                    let node_id_as_u64 = config
                        .node_id
                        .parse::<u64>()
                        .expect("Failed to parse node ID");
                    node_id = Some(node_id_as_u64);
                }
            }
            let environment = env.unwrap_or_default();
            start(node_id, environment).await
        }

        Command::BatchFile {
            file,
            env,
            start_delay,
            proof_interval,
            max_concurrent,
            verbose,
        } => {
            if verbose {
                std::env::set_var("RUST_LOG", "debug");
                env_logger::init();
            }
            let environment = env.unwrap_or_default();
            start_batch_from_file_with_runtime(&file, environment, start_delay, proof_interval, max_concurrent, verbose).await
        }
        
        Command::CreateExamples { dir } => {
            NodeList::create_example_files(&dir)
                .map_err(|e| -> Box<dyn Error> { Box::new(e) })?;

            println!("Example node list files created successfully!");
            println!("Location: {}", dir);
            println!("Edit these files with your actual node IDs, then use:");
            println!("   nexus batch-file --file {}/example_nodes.txt", dir);
            Ok(())
        }

        Command::Logout => {
            let config_path = get_config_path().expect("Failed to get config path");
            clear_node_config(&config_path).map_err(Into::into)
        }
    }
}

async fn start(node_id: Option<u64>, env: Environment) -> Result<(), Box<dyn Error>> {
    if node_id.is_some() {
        start_headless_prover(node_id, env).await
    } else {
        start_with_ui(node_id, env).await
    }
}

async fn start_with_ui(
    node_id: Option<u64>,
    env: Environment,
) -> Result<(), Box<dyn Error>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let res = ui::run(&mut terminal, ui::App::new(node_id, env, crate::orchestrator_client::OrchestratorClient::new(env)));

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;
    
    if let Err(err) = res {
        println!("{err:?}");
    }
    Ok(())
}

async fn start_headless_prover(
    node_id: Option<u64>,
    env: Environment,
) -> Result<(), Box<dyn Error>> {
    println!("Starting Nexus Prover in headless mode...");
    prover::start_prover(env, node_id).await?;
    Ok(())
}

async fn start_batch_from_file_with_runtime(
    file_path: &str,
    env: Environment,
    start_delay: f64,
    proof_interval: u64,
    max_concurrent: usize,
    _verbose: bool,
) -> Result<(), Box<dyn Error>> {
    let node_list = node_list::NodeList::load_from_file(file_path)?;
    let all_nodes = node_list.node_ids().to_vec();

    if all_nodes.is_empty() {
        return Err("Empty node list".into());
    }

    let actual_concurrent = max_concurrent.min(all_nodes.len());

    println!("Nexus Enhanced Runtime Batch Mode");
    println!("Node file: {}", file_path);
    println!("Total nodes: {}", all_nodes.len());
    println!("Max concurrent: {}", actual_concurrent);
    println!("Start delay: {:.1}s, Proof interval: {}s", start_delay, proof_interval);
    println!("Environment: {:?}", env);
    println!("Architecture: High-efficiency prover_runtime");
    println!("Memory optimization: ENABLED");
    println!("HTTP Headers: Randomized residential IPs");  // 新增提示
    println!("---------------------------------------");

    let display = Arc::new(FixedLineDisplay::new(actual_concurrent));
    display.render_display(&HashMap::new()).await;

    let task_pool = Arc::new(TaskPool::new(actual_concurrent));
    let mut join_set = JoinSet::new();
    let (shutdown_sender, _) = broadcast::channel(1);

    for (index, node_id) in all_nodes.iter().take(actual_concurrent).enumerate() {
        let node_id = *node_id;
        let env = env.clone();
        let display = display.clone();
        let shutdown_rx = shutdown_sender.subscribe();

        if index > 0 {
            tokio::time::sleep(std::time::Duration::from_secs_f64(start_delay)).await;
        }

        join_set.spawn(async move {
            let prefix = format!("Node-{}", node_id);
            let display_clone = display.clone();

            let status_callback = Box::new(move |status: String| {
                let display = display_clone.clone();
                let node_id = node_id;
                tokio::spawn(async move {
                    display.update_node_status(node_id, status).await;
                });
            });

            match crate::prover_runtime::run_authenticated_proving_optimized(
                node_id,
                env,
                prefix.clone(),
                proof_interval,
                shutdown_rx,
                Some(status_callback),
            ).await {
                Ok(_) => {
                    display.update_node_status(node_id, "Stopped".to_string()).await;
                }
                Err(e) => {
                    display.update_node_status(node_id, format!("Error: {}", e)).await;
                }
            }

            Ok::<(), prover::ProverError>(())
        });
    }

    monitor_runtime_workers(join_set, display, task_pool).await;

    Ok(())
}

async fn monitor_runtime_workers(
    mut join_set: JoinSet<Result<(), prover::ProverError>>,
    display: Arc<FixedLineDisplay>,
    task_pool: Arc<TaskPool>,
) {
    let mut error_count = 0;
    let start_time = std::time::Instant::now();

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => {
                // Worker completed successfully
            }
            Ok(Err(e)) => {
                error_count += 1;
                println!("Worker error: {}", e);
            }
            Err(e) => {
                error_count += 1;
                println!("Worker panic: {}", e);
            }
        }

        let (active, completed, failed) = task_pool.get_stats();
        let elapsed = start_time.elapsed();
        
        if completed % 10 == 0 || error_count > 0 {
            println!("Runtime Stats - Active: {}, Completed: {}, Failed: {}, Elapsed: {:?}", 
                     active, completed, failed, elapsed);
        }

        display.render_display_optimized().await;
    }

    let final_elapsed = start_time.elapsed();
    let (_, final_completed, final_failed) = task_pool.get_stats();
    
    println!("Batch processing completed!");
    println!("Final Stats - Completed: {}, Failed: {}, Total time: {:?}", 
             final_completed, final_failed, final_elapsed);
}
