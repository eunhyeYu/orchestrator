mod manager;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use manager::{
    BinaryManager, RestartPolicy, StartRequest as CoreStartRequest, StatusFilter,
    StopMode, StopRequest as CoreStopRequest, StopTarget,
};
use tonic::{transport::Server, Request, Response, Status};

use logging_tracing::{Level, LogAndTraceBuilder};

pub mod lifecycle {
    tonic::include_proto!("lifecycle");
}

use lifecycle::binary_lifecycle_server::{BinaryLifecycle, BinaryLifecycleServer};
use lifecycle::{ManagerStats, ProcessInfo, StartResponse, StatusResponse, StopResponse};

const DEFAULT_GRPC_PORT: u16 = 50051;
const MONITOR_INTERVAL_SECS: u64 = 1;

pub struct BinaryLifecycleService {
    manager: Arc<Mutex<BinaryManager>>,
}

impl BinaryLifecycleService {
    fn new(manager: Arc<Mutex<BinaryManager>>) -> Self {
        Self { manager }
    }
}

#[tonic::async_trait]
impl BinaryLifecycle for BinaryLifecycleService {
    async fn start_binary(
        &self,
        request: Request<lifecycle::StartRequest>,
    ) -> Result<Response<StartResponse>, Status> {
        let req = request.into_inner();
        let policy = match lifecycle::RestartPolicy::try_from(req.restart_policy)
            .unwrap_or(lifecycle::RestartPolicy::Never)
        {
            lifecycle::RestartPolicy::Never => RestartPolicy::Never,
            lifecycle::RestartPolicy::OnFailure => RestartPolicy::OnFailure {
                max_retries: req.max_retries,
                delay: Duration::from_secs(req.restart_delay_secs as u64),
            },
            lifecycle::RestartPolicy::Always => RestartPolicy::Always {
                max_retries: req.max_retries,
                delay: Duration::from_secs(req.restart_delay_secs as u64),
            },
        };

        let result = self
            .manager
            .lock()
            .map_err(|e| Status::internal(format!("Manager mutex poisoned: {}", e)))?
            .start_binary(CoreStartRequest {
                service_name: req.service_name,
                binary_path: req.binary_path,
                args: req.args,
                restart_policy: policy,
            });

        Ok(Response::new(StartResponse {
            success: result.success,
            pid: result.pid,
            instance_id: result.instance_id,
            service_name: result.service_name,
            message: result.message,
        }))
    }

    async fn stop_binary(
        &self,
        request: Request<lifecycle::StopRequest>,
    ) -> Result<Response<StopResponse>, Status> {
        let req = request.into_inner();

        let target = if req.stop_all {
            StopTarget::All
        } else if !req.instance_id.trim().is_empty() {
            StopTarget::InstanceId(req.instance_id)
        } else if !req.service_name.trim().is_empty() {
            StopTarget::ServiceName(req.service_name)
        } else if req.pid > 0 {
            StopTarget::Pid(req.pid)
        } else {
            StopTarget::All
        };

        let mode = if req.force {
            StopMode::Force
        } else {
            StopMode::Graceful
        };

        let timeout = if req.timeout_secs == 0 {
            None
        } else {
            Some(Duration::from_secs(req.timeout_secs as u64))
        };

        let result = self
            .manager
            .lock()
            .map_err(|e| Status::internal(format!("Manager mutex poisoned: {}", e)))?
            .stop_binary(CoreStopRequest {
                target,
                mode,
                timeout,
            });

        Ok(Response::new(StopResponse {
            success: result.success,
            stopped_count: result.stopped_count,
            message: result.message,
        }))
    }

    async fn get_status(
        &self,
        request: Request<lifecycle::StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let req = request.into_inner();
        let filter = if !req.instance_id.trim().is_empty() {
            StatusFilter::InstanceId(req.instance_id)
        } else if !req.service_name.trim().is_empty() {
            StatusFilter::ServiceName(req.service_name)
        } else if req.pid > 0 {
            StatusFilter::Pid(req.pid)
        } else {
            StatusFilter::All
        };

        let manager = self.manager.lock()
            .map_err(|e| Status::internal(format!("Manager mutex poisoned: {}", e)))?;
        let statuses = manager.get_status(filter);
        let stats = manager.get_stats().clone();

        let processes = statuses
            .into_iter()
            .map(|s| ProcessInfo {
                pid: s.pid,
                instance_id: s.instance_id,
                service_name: s.service_name,
                binary_path: s.binary_path,
                state: s.state,
                uptime_secs: s.uptime_secs,
                memory_kb: s.memory_kb,
                restart_count: s.restart_count,
            })
            .collect();

        Ok(Response::new(StatusResponse {
            processes,
            stats: Some(ManagerStats {
                total_started: stats.total_started,
                total_stopped: stats.total_stopped,
                total_completed: stats.total_completed,
                total_crashed: stats.total_crashed,
                total_restarted: stats.total_restarted,
            }),
        }))
    }
}

fn start_grpc_server(manager: Arc<Mutex<BinaryManager>>) {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .expect("failed to build tokio runtime");

        rt.block_on(async move {
            let addr = format!("0.0.0.0:{DEFAULT_GRPC_PORT}")
                .parse()
                .expect("invalid grpc address");

            if let Err(e) = Server::builder()
                .add_service(BinaryLifecycleServer::new(BinaryLifecycleService::new(manager)))
                .serve(addr)
                .await
            {
                eprintln!("FATAL: gRPC server failed: {}", e);
                eprintln!("Server cannot continue without gRPC. Exiting.");
                std::process::exit(1);
            }
        });
    });
}

// Kyron 태스크로 실행되는 모니터링 루프
async fn monitor_task(manager: Arc<Mutex<BinaryManager>>) {
    loop {
        kyron::futures::sleep::sleep(Duration::from_secs(MONITOR_INTERVAL_SECS)).await;
        
        if let Ok(mut mgr) = manager.lock() {
            mgr.check_and_reap();
        }
    }
}

fn main() {
    let _logger = LogAndTraceBuilder::new()
        .global_log_level(Level::INFO)
        .enable_logging(true)
        .build()
        .expect("failed to init logging");

    // ManagerConfig 설정 (커스터마이징 가능)
    let config = manager::ManagerConfig::default();
    // 또는 커스텀 설정:
    // let config = manager::ManagerConfig {
    //     graceful_timeout: Duration::from_secs(10),
    //     max_history_size: 5000,
    //     max_spawn_failures: 5,
    //     history_log_path: Some(PathBuf::from("/var/log/grpc_lifecycle_history.jsonl")),
    // };
    
    let (manager_instance, _log_handle) = BinaryManager::new(config);
    let manager = Arc::new(Mutex::new(manager_instance));

    start_grpc_server(Arc::clone(&manager));

    // Kyron runtime 생성
    let (builder, _engine_id) = kyron::runtime::RuntimeBuilder::new().with_engine(
        kyron::runtime::ExecutionEngineBuilder::new()
            .task_queue_size(256)
            .workers(2),
    );

    let mut runtime = builder.build().expect("Failed to build kyron runtime");

    // Kyron 태스크로 모니터링 루프 spawn
    let manager_monitor = Arc::clone(&manager);
    
    runtime.block_on(async move {
        kyron::spawn(async move {
            monitor_task(manager_monitor).await;
        });

        // 메인 루프 - 무한 대기
        loop {
            kyron::futures::sleep::sleep(Duration::from_secs(3600)).await;
        }
    });
}
