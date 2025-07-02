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
use std::io::{self, Write}; // 修复: 使用标准库的Write
use std::fmt::Write as FmtWrite; // 修复: 区分格式化Write
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
    #[allow(dead_code)]  // 添加这行
    initial_rss: AtomicU64,
    peak_rss: AtomicU64,
    #[allow(dead_code)]  // 添加这行
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
    #[allow(dead_code)]  // 添加这行
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
    
    #[allow(dead_code)]  // 添加这行
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

// 修复Debug trait for StatusCache
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
    #[allow(dead_code)]  // 添加这行
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
    string_pool: Arc<StringPool>,
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
        // Convert to CompactString for better memory efficiency
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

        // Enhanced memory defragmentation check
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

        // Legacy memory pressure check as backup
        if crate::utils::system::check_memory_pressure() {
            println!("High memory usage detected, performing additional cleanup...");
            crate::utils::system::perform_memory_cleanup();
        }

        // Optimized hash calculation using parallel processing for large datasets
        let current_hash = if lines.len() > 100 {
            // Use parallel hashing for large datasets
            lines.par_iter()
                .map(|(id, status)| {
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    hasher.write_u64(*id);
                    hasher.write(status.as_bytes());
                    hasher.finish()
                })
                .reduce(|| 0, |a, b| a ^ b)
        } else {
            // Use sequential hashing for small datasets
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
        // 修复: 使用print!而不是异步写入
        print!("\x1b[2J\x1b[H");

        // Use string pool for time formatting
        let time_buffer = format!("{}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S"));
        let _ = write!(time_buffer, "{}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S"));

        // Title - 修复: 移除emoji，使用ASCII字符
        println!("Nexus Enhanced Batch Mining Monitor - {}", time_buffer);
        println!("=======================================");

        // Optimized statistics calculation using parallel processing
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

        // Enhanced memory statistics
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

        // Optimized sorting using SmallVec for better performance with small collections
        let mut sorted_lines: SmallVec<[_; 32]> = SmallVec::with_capacity(lines.len());
        sorted_lines.extend(lines.iter());
        sorted_lines.sort_unstable_by_key(|(id, _)| *id);

        // Write node statuses
        for (node_id, status) in sorted_lines.iter() {
            println!("Node-{}: {}", node_id, status);
        }

        println!("---------------------------------------");
        println!("Press Ctrl+C to stop all miners");

        // Return string to pool
        let time_buffer = format!("{}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S"));

        // 修复: 使用标准库flush
        let _ = io::stdout().flush();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logger with optimized settings
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

/// Starts the Nexus CLI application.
async fn start(node_id: Option<u64>, env: Environment) -> Result<(), Box<dyn Error>> {
    if node_id.is_some() {
        start_headless_prover(node_id, env).await
    } else {
        start_with_ui(node_id, env).await
    }
}

/// Start with UI (original logic)
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

/// High-efficiency batch launcher using optimized prover_runtime architecture
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
    println!("---------------------------------------");

    // Create optimized display manager
    let display = Arc::new(FixedLineDisplay::new(actual_concurrent));
    display.render_display(&HashMap::new()).await;

    // Use optimized task pool
    let task_pool = Arc::new(TaskPool::new(actual_concurrent));
    let mut join_set = JoinSet::new(); // 修复: 正确的JoinSet类型
    let (shutdown_sender, _) = broadcast::channel(1);

    for (index, node_id) in all_nodes.iter().take(actual_concurrent).enumerate() {
        let node_id = *node_id;
        let env = env.clone();
        let display = display.clone();
        let shutdown_rx = shutdown_sender.subscribe();

        // Add startup delay
        if index > 0 {
            tokio::time::sleep(std::time::Duration::from_secs_f64(start_delay)).await;
        }

        // 修复: 直接spawn任务而不是嵌套JoinHandle
        join_set.spawn(async move {
            let prefix = format!("Node-{}", node_id);
            let display_clone = display.clone();

            // Create status callback for fixed position display
            let status_callback = Box::new(move |status: String| {
                let display = display_clone.clone();
                let node_id = node_id;
                tokio::spawn(async move {
                    display.update_node_status(node_id, status).await;
                });
            });

            // Start memory-optimized authenticated proving loop
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

    // Monitor and error handling
    monitor_runtime_workers(join_set, display, task_pool).await;

    Ok(())
}

/// Monitor optimized runtime workers - 修复JoinSet类型
async fn monitor_runtime_workers(
    mut join_set: JoinSet<Result<(), prover::ProverError>>, // 修复: 正确的类型
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

        // Update display with current statistics
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
