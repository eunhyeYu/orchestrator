use std::fs;
use std::path::Path;
use std::thread;
use std::time::Duration;

const STATE_FILE: &str = "/tmp/counter_file_resume_state.txt";
const MAX_VALUE: i32 = 100;
const INTERVAL_SECS: f64 = 1.0;

fn read_state(path: &str) -> i32 {
    match fs::read_to_string(path) {
        Ok(content) => content.trim().parse::<i32>().unwrap_or(0),
        Err(_) => 0,
    }
}

fn write_state(path: &str, value: i32) -> Result<(), String> {
    if let Some(parent) = Path::new(path).parent() {
        fs::create_dir_all(parent).map_err(|e| format!("failed to create state dir: {e}"))?;
    }
    fs::write(path, format!("{value}\n")).map_err(|e| format!("failed to write state: {e}"))
}

fn main() {
    let last = read_state(STATE_FILE);
    let start = (last + 1).min(MAX_VALUE).max(1);

    println!(
        "[counter] state_file={} last={} start={} max={}",
        STATE_FILE, last, start, MAX_VALUE
    );

    for value in start..=MAX_VALUE {
        println!("{value}");

        if let Err(e) = write_state(STATE_FILE, value) {
            eprintln!("[counter] {e}");
            std::process::exit(2);
        }

        if value < MAX_VALUE {
            thread::sleep(Duration::from_secs_f64(INTERVAL_SECS));
        }
    }

    if let Err(e) = write_state(STATE_FILE, 0) {
        eprintln!("[counter] {e}");
        std::process::exit(2);
    }

    println!("[counter] completed; state reset to 0");
}
