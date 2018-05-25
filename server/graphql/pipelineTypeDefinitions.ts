let typeDefinitions = `
type PageInfo {
    endCursor: String
    hasNextPage: Boolean
}

type ExecutionEdge {
    node: TaskExecution
    cursor: String
}

type ExecutionConnection {
    totalCount: Int
    pageInfo: PageInfo
    edges: [ExecutionEdge]
}

type ExecutionPage {
    offset: Int
    limit: Int
    totalCount: Int
    hasNextPage: Boolean
    items: [TaskExecution]
}

type PipelineWorker {
  id: String!
  worker_id: String
  name: String
  os_type: String
  platform: String
  arch: String
  release: String
  cpu_count: Int
  total_memory: Float
  free_memory: Float
  load_average: Float
  work_unit_capacity: Float
  last_seen: Float
  task_load: Float
  status: Int
  is_in_scheduler_pool: Boolean
  is_cluster_proxy: Boolean
  created_at: Float
  updated_at: Float
  deleted_at: Float
}

type TaskRepository {
  id: String!
  name: String!
  description: String!
  location: String!
  task_definitions: [TaskDefinition!]!
  created_at: Float
  updated_at: Float
  deleted_at: Float
}

type TaskDefinition {
  id: String!
  name: String!
  description: String!
  script: String!
  interpreter: String!
  script_args: String!
  cluster_args: String
  expected_exit_code: Int
  work_units: Float
  cluster_work_units: Float
  log_prefix: String
  task_repository_id: String
  task_repository: TaskRepository
  pipeline_stages: [PipelineStage!]!
  script_status: Boolean
  created_at: Float
  updated_at: Float
  deleted_at: Float
}

type TaskExecution {
  id: String
  worker_id: String
  task_id: String
  task_definition_id: String
  task_definition: TaskDefinition
  pipeline_stage_id: String
  pipeline_stage: PipelineStage
  work_units: Float
  cluster_work_units: Float
  resolved_script: String
  resolved_interpreter: String
  resolved_script_args: String
  resolved_cluster_args: String
  resolved_log_path: String
  execution_status_code: Int
  completion_status_code: Int
  last_process_status_code: Float
  max_memory: Float
  max_cpu: Float
  exit_code: Int
  started_at: String
  completed_at: String
  created_at: String
  updated_at: String
  deleted_at: String
}

type Project {
  id: String!
  name: String
  description: String
  root_path: String
  log_root_path: String
  dashboard_json_status: Boolean
  sample_number: Int
  sample_x_min: Int
  sample_x_max: Int
  sample_y_min: Int
  sample_y_max: Int
  sample_z_min: Int
  sample_z_max: Int
  region_x_min: Int
  region_x_max: Int
  region_y_min: Int
  region_y_max: Int
  region_z_min: Int
  region_z_max: Int
  is_processing: Boolean
  created_at: Float
  updated_at: Float
  deleted_at: Float
  stages: [PipelineStage]
}

type PipelineTileStatus {
  incomplete: Int
  queued: Int
  processing: Int
  complete: Int
  failed: Int
  canceled: Int
}

type PipelineStagePerformance {
  id: String!
  pipeline_stage_id: String
  num_in_process: Int
  num_ready_to_process: Int
  num_execute: Int
  num_complete: Int
  num_error: Int
  num_cancel: Int
  cpu_average: Float
  cpu_high: Float
  cpu_low: Float
  memory_average: Float
  memory_high: Float
  memory_low: Float
  duration_average: Float
  duration_high: Float
  duration_low: Float
  created_at: String
  updated_at: String
  deleted_at: String
}

type PipelineStage {
  id: String!
  name: String
  description: String
  function_type: Int
  execution_order: Int
  dst_path: String
  depth: Int
  is_processing: Boolean
  project_id: String
  task_id: String
  previous_stage_id: String
  project: Project
  task: TaskDefinition
  tile_status: PipelineTileStatus
  performance: PipelineStagePerformance
  previous_stage: PipelineStage
  child_stages: [PipelineStage]
  created_at: Float
  updated_at: Float
  deleted_at: Float
}

type Tile {
  relative_path: String
  lat_x: Int
  lat_y: Int
  lat_z: Int
  prev_stage_status: Int
  this_stage_status: Int
}

type TilePage {
    offset: Int
    limit: Int
    totalCount: Int
    hasNextPage: Boolean
    items: [Tile]
}

type TileStageStatus {
  relative_path: String
  stage_id: String
  depth: Int
  status: Int
}

type TileStatus {
  x_index: Int
  y_index: Int
  stages: [TileStageStatus]
}

type TilePlane {
  max_depth: Int
  x_min: Int
  x_max: Int
  y_min: Int
  y_max: Int
  tiles: [TileStatus]
}

type MutateProjectOutput {
    project: Project
    error: String
}

type DeleteProjectOutput {
    id: String
    error: String
}

type MutatePipelineStageOutput {
    pipelineStage: PipelineStage
    error: String
}

type DeletePipelineStageOutput {
    id: String
    error: String
}

type MutateTaskRepositoryOutput {
    taskRepository: TaskRepository
    error: String
}

type DeleteTaskRepositoryOutput {
    id: String
    error: String
}

type MutateTaskDefinitionOutput {
    taskDefinition: TaskDefinition
    error: String
}

type DeleteTaskDefinitionOutput {
    id: String
    error: String
}

type MutatePipelineWorkerOutput {
    worker: PipelineWorker
    error: String
}

input RegionInput {
  x_min: Int
  x_max: Int
  y_min: Int
  y_max: Int
  z_min: Int
  z_max: Int
}

input ProjectInput {
  id: String
  name: String
  description: String
  root_path: String
  log_root_path: String
  sample_number: Int
  is_processing: Boolean
  region_bounds: RegionInput
}

input PipelineStageInput {
  id: String
  name: String
  description: String
  function_type: Int
  execution_order: Int
  dst_path: String
  depth: Int
  is_processing: Boolean
  project_id: String
  previous_stage_id: String
  task_id: String
}

input TaskRepositoryInput {
    id: String
    name: String
    location: String
    description: String
}

input TaskDefinitionInput {
  id: String
  name: String
  description: String
  script: String
  interpreter: String
  task_repository_id: String
  script_args: String
  cluster_args: String
  expected_exit_code: Int
  work_units: Float
  cluster_work_units: Float
  log_prefix: String
}

input PipelineWorkerInput {
  id: String
  work_unit_capacity: Float
  is_cluster_proxy: Boolean
}

type Query {
  pipelineWorker(id: String!): PipelineWorker
  pipelineWorkers: [PipelineWorker!]!
  
  project(id: String!): Project
  projects: [Project!]!
  
  pipelineStage(id: String!): PipelineStage
  pipelineStages: [PipelineStage!]!
  pipelineStagesForProject(id: String!): [PipelineStage!]!
  
  taskDefinition(id: String!): TaskDefinition
  taskDefinitions: [TaskDefinition!]!
  
  taskRepository(id: String!): TaskRepository
  taskRepositories: [TaskRepository!]!
  
  taskExecution(id: String!): TaskExecution
  taskExecutions: [TaskExecution!]!
  taskExecutionsPage(offset: Int, limit: Int, status: Int): ExecutionPage

  pipelineStagePerformance(id: String!): PipelineStagePerformance
  pipelineStagePerformances: [PipelineStagePerformance!]!
  
  projectPlaneTileStatus(project_id: String, plane: Int): TilePlane
  
  tilesForStage(pipelineStageId: String, status: Int, offset: Int, limit: Int): TilePage
  
  scriptContents(task_definition_id: String): String
  
  pipelineVolume: String
}

type Mutation {
  createProject(project: ProjectInput): MutateProjectOutput
  updateProject(project: ProjectInput): MutateProjectOutput
  duplicateProject(id: String): MutateProjectOutput
  deleteProject(id: String!): DeleteProjectOutput
  
  createPipelineStage(pipelineStage: PipelineStageInput): MutatePipelineStageOutput
  updatePipelineStage(pipelineStage: PipelineStageInput): MutatePipelineStageOutput
  deletePipelineStage(id: String!): DeletePipelineStageOutput
  
  createTaskRepository(taskRepository: TaskRepositoryInput): MutateTaskRepositoryOutput
  updateTaskRepository(taskRepository: TaskRepositoryInput): MutateTaskRepositoryOutput
  deleteTaskRepository(id: String!): DeleteTaskRepositoryOutput
  
  createTaskDefinition(taskDefinition: TaskDefinitionInput): MutateTaskDefinitionOutput
  updateTaskDefinition(taskDefinition: TaskDefinitionInput): MutateTaskDefinitionOutput
  deleteTaskDefinition(id: String!): DeleteTaskDefinitionOutput

  setWorkerAvailability(id: String!, shouldBeInSchedulerPool: Boolean!): PipelineWorker
  updateWorker(worker: PipelineWorkerInput): MutatePipelineWorkerOutput
  
  setTileStatus(pipelineStageId: String, tileIds: [String], status: Int): [Tile]
  convertTileStatus(pipelineStageId: String, currentStatus: Int, desiredStatus: Int): [Tile]
}

schema {
  query: Query
  mutation: Mutation
}
`;

export default typeDefinitions;
