import {CompletionResult, ExecutionStatus, SyncStatus} from "./taskExecution";

export type WorkerTaskExecution = {
    id: string;
    worker_id: string;
    remote_task_execution_id: string;
    tile_id: string;
    task_definition_id: string;
    pipeline_stage_id: string;
    queue_type: number;
    local_work_units: number;
    cluster_work_units: number;
    resolved_output_path: string;
    resolved_script: string;
    resolved_interpreter: string;
    resolved_script_args: string;
    resolved_cluster_args: string;
    resolved_log_path: string;
    expected_exit_code: number;
    job_id: number;
    job_name: string;
    execution_status_code: ExecutionStatus;
    completion_status_code: CompletionResult;
    last_process_status_code: number;
    cpu_time_seconds: number;
    max_cpu_percent: number;
    max_memory_mb: number;
    exit_code: number;
    submitted_at: Date;
    started_at: Date;
    completed_at: Date;
    sync_status: SyncStatus;
    synchronized_at: Date;
    created_at: Date;
    updated_at: Date;
}