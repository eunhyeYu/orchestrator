use clap::{Parser, Subcommand};

pub mod lifecycle {
    tonic::include_proto!("lifecycle");
}

use lifecycle::binary_lifecycle_client::BinaryLifecycleClient;
use lifecycle::{RestartPolicy, StartRequest, StatusRequest, StopRequest};

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    endpoint: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Start {
        #[arg(long)]
        service: Option<String>,
        /// Binary path (required)
        #[arg(long)]
        binary: String,
        #[arg(long, num_args = 0.., allow_hyphen_values = true)]
        args: Vec<String>,
        #[arg(long, default_value = "never")]
        policy: String,
        #[arg(long, default_value_t = 0)]
        max_retries: u32,
        #[arg(long, default_value_t = 0)]
        delay_secs: u32,
    },
    Stop {
        #[arg(long)]
        pid: Option<u32>,
        #[arg(long)]
        instance: Option<String>,
        #[arg(long)]
        service: Option<String>,
        #[arg(long, default_value_t = false)]
        all: bool,
        #[arg(long, default_value_t = false)]
        force: bool,
        #[arg(long, default_value_t = 5)]
        timeout_secs: u32,
    },
    Status {
        #[arg(long)]
        pid: Option<u32>,
        #[arg(long)]
        instance: Option<String>,
        #[arg(long)]
        service: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let mut client = BinaryLifecycleClient::connect(cli.endpoint).await?;

    match cli.command {
        Commands::Start {
            service,
            binary,
            args,
            policy,
            max_retries,
            delay_secs,
        } => {
            let restart_policy = parse_policy(&policy);
            let resp = client
                .start_binary(StartRequest {
                    service_name: service.unwrap_or_default(),
                    binary_path: binary,
                    args,
                    restart_policy,
                    max_retries,
                    restart_delay_secs: delay_secs,
                })
                .await?
                .into_inner();

            println!(
                "success={} pid={} instance_id={} service={} message={}",
                resp.success, resp.pid, resp.instance_id, resp.service_name, resp.message
            );
        }
        Commands::Stop {
            pid,
            instance,
            service,
            all,
            force,
            timeout_secs,
        } => {
            let resp = client
                .stop_binary(StopRequest {
                    pid: pid.unwrap_or(0),
                    instance_id: instance.unwrap_or_default(),
                    service_name: service.unwrap_or_default(),
                    stop_all: all,
                    force,
                    timeout_secs,
                })
                .await?
                .into_inner();

            println!(
                "success={} stopped_count={} message={}",
                resp.success, resp.stopped_count, resp.message
            );
        }
        Commands::Status {
            pid,
            instance,
            service,
        } => {
            let resp = client
                .get_status(StatusRequest {
                    pid: pid.unwrap_or(0),
                    instance_id: instance.unwrap_or_default(),
                    service_name: service.unwrap_or_default(),
                })
                .await?
                .into_inner();

            for p in resp.processes {
                println!(
                    "pid={} instance={} service={} state={} uptime={:.2}s mem={}KB restarts={} binary={}",
                    p.pid,
                    p.instance_id,
                    p.service_name,
                    p.state,
                    p.uptime_secs,
                    p.memory_kb,
                    p.restart_count,
                    p.binary_path
                );
            }

            if let Some(stats) = resp.stats {
                println!(
                    "stats started={} stopped={} completed={} crashed={} restarted={}",
                    stats.total_started,
                    stats.total_stopped,
                    stats.total_completed,
                    stats.total_crashed,
                    stats.total_restarted
                );
            }
        }
    }

    Ok(())
}

fn parse_policy(policy: &str) -> i32 {
    match policy.to_lowercase().as_str() {
        "always" => RestartPolicy::Always as i32,
        "onfailure" | "on_failure" | "on-failure" => RestartPolicy::OnFailure as i32,
        _ => RestartPolicy::Never as i32,
    }
}
