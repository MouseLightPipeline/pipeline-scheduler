export = {
    up: async (queryInterface, Sequelize) => {
        await queryInterface.createTable(
            "TaskRepositories",
            {
                id: {
                    primaryKey: true,
                    type: Sequelize.UUID,
                    defaultValue: Sequelize.UUIDV4
                },
                name: Sequelize.TEXT,
                description: Sequelize.TEXT,
                location: Sequelize.TEXT,
                created_at: Sequelize.DATE,
                updated_at: Sequelize.DATE,
                deleted_at: Sequelize.DATE
            });

        await queryInterface.createTable(
            "TaskDefinitions",
            {
                id: {
                    primaryKey: true,
                    type: Sequelize.UUID,
                    defaultValue: Sequelize.UUIDV4
                },
                name: Sequelize.TEXT,
                description: Sequelize.TEXT,
                script: Sequelize.TEXT,
                interpreter: Sequelize.TEXT,
                script_args: Sequelize.TEXT,
                cluster_args: Sequelize.TEXT,
                expected_exit_code: Sequelize.INTEGER,
                work_units: Sequelize.DOUBLE,
                cluster_work_units: Sequelize.DOUBLE,
                log_prefix: Sequelize.TEXT,
                task_repository_id: {
                    type: Sequelize.UUID,
                    references: {
                        model: "TaskRepositories",
                        key: "id"
                    }
                },
                created_at: Sequelize.DATE,
                updated_at: Sequelize.DATE,
                deleted_at: Sequelize.DATE
            });

        await queryInterface.createTable(
            "PipelineWorkers",
            {
                id: {
                    primaryKey: true,
                    type: Sequelize.UUID,
                    defaultValue: Sequelize.UUIDV4
                },
                worker_id: Sequelize.UUID,
                name: Sequelize.TEXT,
                address: Sequelize.TEXT,
                port: Sequelize.INTEGER,
                os_type: Sequelize.TEXT,
                platform: Sequelize.TEXT,
                arch: Sequelize.TEXT,
                release: Sequelize.TEXT,
                cpu_count: Sequelize.INTEGER,
                total_memory: Sequelize.DOUBLE,
                free_memory: Sequelize.DOUBLE,
                load_average: Sequelize.DOUBLE,
                work_unit_capacity: Sequelize.DOUBLE,
                is_in_scheduler_pool: Sequelize.BOOLEAN,
                is_cluster_proxy: Sequelize.BOOLEAN,
                last_seen: Sequelize.DATE,
                created_at: Sequelize.DATE,
                updated_at: Sequelize.DATE,
                deleted_at: Sequelize.DATE
            });

        await queryInterface.createTable(
            "Projects",
            {
                id: {
                    primaryKey: true,
                    type: Sequelize.UUID,
                    defaultValue: Sequelize.UUIDV4
                },
                name: Sequelize.TEXT,
                description: Sequelize.TEXT,
                root_path: Sequelize.TEXT,
                log_root_path: Sequelize.TEXT,
                sample_number: Sequelize.INTEGER,
                sample_x_min: Sequelize.INTEGER,
                sample_x_max: Sequelize.INTEGER,
                sample_y_min: Sequelize.INTEGER,
                sample_y_max: Sequelize.INTEGER,
                sample_z_min: Sequelize.INTEGER,
                sample_z_max: Sequelize.INTEGER,
                region_x_min: Sequelize.INTEGER,
                region_x_max: Sequelize.INTEGER,
                region_y_min: Sequelize.INTEGER,
                region_y_max: Sequelize.INTEGER,
                region_z_min: Sequelize.INTEGER,
                region_z_max: Sequelize.INTEGER,
                is_processing: Sequelize.BOOLEAN,
                created_at: Sequelize.DATE,
                updated_at: Sequelize.DATE,
                deleted_at: Sequelize.DATE
            });

        await queryInterface.createTable(
            "PipelineStages",
            {
                id: {
                    primaryKey: true,
                    type: Sequelize.UUID,
                    defaultValue: Sequelize.UUIDV4
                },
                name: Sequelize.TEXT,
                description: Sequelize.TEXT,
                function_type: Sequelize.INTEGER,
                dst_path: Sequelize.TEXT,
                is_processing: Sequelize.BOOLEAN,
                depth: Sequelize.INTEGER,
                project_id: {
                    type: Sequelize.UUID,
                    references: {
                        model: "Projects",
                        key: "id"
                    }
                },
                task_id: {
                    type: Sequelize.UUID,
                    references: {
                        model: "TaskDefinitions",
                        key: "id"
                    }
                },
                previous_stage_id: {
                    type: Sequelize.UUID,
                    references: {
                        model: "PipelineStages",
                        key: "id"
                    }
                },
                created_at: Sequelize.DATE,
                updated_at: Sequelize.DATE,
                deleted_at: Sequelize.DATE
            });

        await queryInterface.createTable(
            "PipelineStagePerformances",
            {
                id: {
                    primaryKey: true,
                    type: Sequelize.UUID,
                    defaultValue: Sequelize.UUIDV4
                },
                num_in_process: Sequelize.INTEGER,
                num_ready_to_process: Sequelize.INTEGER,
                num_execute: Sequelize.INTEGER,
                num_complete: Sequelize.INTEGER,
                num_error: Sequelize.INTEGER,
                num_cancel: Sequelize.INTEGER,
                duration_average: Sequelize.DOUBLE,
                duration_high: Sequelize.DOUBLE,
                duration_low: Sequelize.DOUBLE,
                cpu_average: Sequelize.DOUBLE,
                cpu_high: Sequelize.DOUBLE,
                cpu_low: Sequelize.DOUBLE,
                memory_average: Sequelize.DOUBLE,
                memory_high: Sequelize.DOUBLE,
                memory_low: Sequelize.DOUBLE,
                pipeline_stage_id: {
                    type: Sequelize.UUID,
                    references: {
                        model: "PipelineStages",
                        key: "id"
                    }
                },
                created_at: Sequelize.DATE,
                updated_at: Sequelize.DATE,
                deleted_at: Sequelize.DATE
            });

        await queryInterface.createTable(
            "PipelineStageFunctions",
            {
                id: {
                    primaryKey: true,
                    type: Sequelize.INTEGER,
                    defaultValue: Sequelize.UUIDV4
                },
                name: Sequelize.TEXT,
                created_at: Sequelize.DATE,
                updated_at: Sequelize.DATE,
                deleted_at: Sequelize.DATE
            });
/*
        await queryInterface.createTable("TaskExecutions",
            {
                id: {
                    primaryKey: true,
                    type: Sequelize.UUID,
                    defaultValue: Sequelize.UUIDV4
                },
                worker_id: Sequelize.UUID,
                work_units: Sequelize.DOUBLE,
                cluster_work_units: Sequelize.DOUBLE,
                tile_id: Sequelize.TEXT,
                resolved_script: Sequelize.TEXT,
                resolved_interpreter: Sequelize.TEXT,
                resolved_script_args: Sequelize.TEXT,
                resolved_cluster_args: Sequelize.TEXT,
                resolved_log_path: Sequelize.TEXT,
                expected_exit_code: Sequelize.INTEGER,
                queue_type: Sequelize.INTEGER,
                job_id: Sequelize.INTEGER,
                job_name: Sequelize.TEXT,
                execution_status_code: Sequelize.INTEGER,
                completion_status_code: Sequelize.INTEGER,
                last_process_status_code: Sequelize.INTEGER,
                exit_code: Sequelize.INTEGER,
                max_memory: Sequelize.DOUBLE,
                max_cpu: Sequelize.DOUBLE,
                started_at: Sequelize.DATE,
                submitted_at: Sequelize.DATE,
                completed_at: Sequelize.DATE,
                sync_status: Sequelize.INTEGER,
                synchronized_at: Sequelize.DATE,
                registered_at: Sequelize.DATE,
                created_at: Sequelize.DATE,
                updated_at: Sequelize.DATE,
                deleted_at: Sequelize.DATE,
                task_definition_id: {
                    type: Sequelize.UUID,
                    references: {
                        model: "TaskDefinitions",
                        key: "id"
                    }
                },
                pipeline_stage_id: {
                    type: Sequelize.UUID,
                    references: {
                        model: "PipelineStages",
                        key: "id"
                    }
                }
            });*/
    },

    down: async (queryInterface, Sequelize) => {
        await queryInterface.dropTable("PipelineStageFunctions");
        await queryInterface.dropTable("PipelineStagePerformances");
        await queryInterface.dropTable("PipelineStages");
        await queryInterface.dropTable("Projects");
        await queryInterface.dropTable("PipelineWorkers");
        await queryInterface.dropTable("TaskDefinitions");
        await queryInterface.dropTable("TaskRepositories");
        // await queryInterface.dropTable("TaskExecutions");
    }
};
