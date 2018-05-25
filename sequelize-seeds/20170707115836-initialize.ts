import * as path from "path";

const seedEnv = process.env.PIPELINE_SEED_ENV || "production";
const isProduction = seedEnv === "production";

const taskLocationPrefix = process.env.PIPELINE_VOLUME || "/groups/mousebrainmicro/mousebrainmicro/Software/pipeline/pipeline-task-definitions/default";

enum TaskArgumentType {
    Literal = 0,
    Parameter = 1
}

export = {
    up: async (queryInterface, Sequelize) => {
        const when = new Date();

        await queryInterface.bulkInsert("TaskRepositories", createTaskRepositories(when), {});
        await queryInterface.bulkInsert("TaskDefinitions", createTaskDefinitions(when), {});
        await queryInterface.bulkInsert("Projects", createProjects(when), {});
        await queryInterface.bulkInsert("PipelineStages", createPipelineStages(when), {});
        await queryInterface.bulkInsert("PipelineStageFunctions", createPipelineStageFunction(when), {});
    },

    down: async (queryInterface, Sequelize) => {
        await queryInterface.bulkDelete("PipelineStageFunctions", null, {});
        await queryInterface.bulkDelete("PipelineStages", null, {});
        await queryInterface.bulkDelete("Projects", null, {});
        await queryInterface.bulkDelete("TaskDefinitions", null, {});
        await queryInterface.bulkDelete("askRepositories", null, {});
    }
};

function createTaskRepositories(when: Date) {
    if (isProduction) {
        return [{
            id: "04dbaad7-9e59-4d9e-b7b7-ae3cd1248ef9",
            name: "Default",
            description: "Default task repository.",
            location: path.join(taskLocationPrefix, "taskdefinitions/default"),
            created_at: when
        }];
    } else {
        return [{
            id: "04dbaad7-9e59-4d9e-b7b7-ae3cd1248ef9",
            name: "Default",
            description: "Default task repository.",
            location: path.join(taskLocationPrefix, "taskdefinitions/default"),
            created_at: when
        }, {
            id: "f22c6e43-782c-4e0e-b0ca-b34fcec3340a",
            name: "Development",
            description: "Development task repository.",
            location: path.join(taskLocationPrefix, "taskdefinitions/development"),
            created_at: when
        }];
    }
}

function createTaskDefinitions(when: Date) {
    if (isProduction) {
        return [{
            id: "ae111b6e-2187-4e07-8ccf-bc7d425d95af",
            name: "Line Fix",
            description: "",
            script: "lineFix.sh",
            interpreter: "none",
            script_args: JSON.stringify({
                arguments: [{
                    value: "/groups/mousebrainmicro/mousebrainmicro/Software/pipeline/apps/lineFix",
                    type: TaskArgumentType.Literal
                }]
            }),
            cluster_args: JSON.stringify({arguments: ["-n 4 -R\"affinity[core(1)]\""]}),
            expected_exit_code: 0,
            work_units: 4,
            cluster_work_units: 1,
            log_prefix: "lf",
            task_repository_id: "04dbaad7-9e59-4d9e-b7b7-ae3cd1248ef9",
            created_at: when
        }, {
            id: "1161f8e6-29d5-44b0-b6a9-8d3e54d23292",
            name: "Axon UInt16",
            description: "Axon UInt16",
            script: "axon-uint16.sh",
            interpreter: "none",
            script_args: JSON.stringify({
                arguments: [{
                    value: "${EXPECTED_EXIT_CODE}",
                    type: TaskArgumentType.Parameter
                }, {
                    value: "${IS_CLUSTER_JOB}",
                    type: TaskArgumentType.Parameter
                }, {
                    value: "/groups/mousebrainmicro/mousebrainmicro/Software/pipeline/apps/axon-classifier",
                    type: TaskArgumentType.Literal
                }]
            }),
            cluster_args: JSON.stringify({arguments: ["-n 4 -R\"select[broadwell]\""]}),
            expected_exit_code: 0,
            work_units: 18,
            cluster_work_units: 1,
            log_prefix: "ax",
            task_repository_id: "04dbaad7-9e59-4d9e-b7b7-ae3cd1248ef9",
            created_at: when
        }, {
            id: "a9f21399-07c0-425c-86f6-6e4f45bb06b9",
            name: "Descriptor",
            description: "",
            script: "dogDescriptor.sh",
            interpreter: "none",
            script_args: JSON.stringify({
                arguments: [{
                    value: "${EXPECTED_EXIT_CODE}",
                    type: TaskArgumentType.Parameter
                }, {
                    value: "${TASK_ID}",
                    type: TaskArgumentType.Parameter
                }, {
                    value: "/groups/mousebrainmicro/mousebrainmicro/Software/pipeline/apps",
                    type: TaskArgumentType.Literal
                }, {
                    value: "/groups/mousebrainmicro/mousebrainmicro/Software/mcr/v90",
                    type: TaskArgumentType.Literal
                }]
            }),
            cluster_args: JSON.stringify({arguments: ["-n 4 -R\"affinity[core(1)]\""]}),
            expected_exit_code: 0,
            work_units: 4,
            cluster_work_units: 1,
            log_prefix: "dd",
            task_repository_id: "04dbaad7-9e59-4d9e-b7b7-ae3cd1248ef9",
            created_at: when
        }, {
            id: "3ba41d1c-13d0-4def-9b5b-54d940a0fa08",
            name: "Point Match",
            description: "",
            script: "pointMatch.sh",
            interpreter: "none",
            script_args: JSON.stringify({
                arguments: [{
                    value: "${PROJECT_ROOT}",
                    type: TaskArgumentType.Parameter
                }, {
                    value: "${ADJACENT_TILE_RELATIVE_PATH}",
                    type: TaskArgumentType.Parameter
                }, {
                    value: "${EXPECTED_EXIT_CODE}",
                    type: TaskArgumentType.Parameter
                }, {
                    value: "${TASK_ID}",
                    type: TaskArgumentType.Parameter
                }, {
                    value: "/groups/mousebrainmicro/mousebrainmicro/Software/pipeline/apps/pointmatch_test",
                    type: TaskArgumentType.Literal
                }, {
                    value: "/groups/mousebrainmicro/mousebrainmicro/Software/mcr/v92",
                    type: TaskArgumentType.Literal
                }, {
                    value: "[0,0,0]",
                    type: TaskArgumentType.Literal
                }, {
                    value: "1",
                    type: TaskArgumentType.Literal
                }, {
                    value: "10000",
                    type: TaskArgumentType.Literal
                }]
            }),
            cluster_args: JSON.stringify({arguments: ["-n 2 -R\"affinity[core(1)]\""]}),
            expected_exit_code: 0,
            work_units: 1,
            cluster_work_units: 1,
            log_prefix: "pm",
            task_repository_id: "04dbaad7-9e59-4d9e-b7b7-ae3cd1248ef9",
            created_at: when
        }];
    } else {
        return [{
            id: "04b8313e-0e96-4194-9c06-22771acd3986",
            name: "Echo",
            description: "Simple command to test shell worker execution.  Will echo all arguments.",
            script: "echo.sh",
            interpreter: "none",
            script_args: JSON.stringify({
                arguments: [{
                    value: `"custom arg 1"`,
                    type: TaskArgumentType.Literal
                }, {value: `"custom arg 2"`, type: TaskArgumentType.Literal}]
            }),
            cluster_args: JSON.stringify({arguments: [""]}),
            expected_exit_code: 0,
            work_units: 0,
            cluster_work_units: 1,
            log_prefix: "ec",
            task_repository_id: "f22c6e43-782c-4e0e-b0ca-b34fcec3340a",
            created_at: when
        }, {
            id: "ae111b6e-2187-4e07-8ccf-bc7d425d95af",
            name: "Line Fix",
            description: "",
            script: "lineFix.sh",
            interpreter: "none",
            script_args: JSON.stringify({
                arguments: [{
                    value: "/groups/mousebrainmicro/mousebrainmicro/Software/pipeline/apps/lineFix",
                    type: TaskArgumentType.Literal
                }]
            }),
            cluster_args: JSON.stringify({arguments: ["-n 4 -R\"affinity[core(1)]\""]}),
            expected_exit_code: 0,
            work_units: 4,
            cluster_work_units: 1,
            log_prefix: "lf",
            task_repository_id: "04dbaad7-9e59-4d9e-b7b7-ae3cd1248ef9",
            created_at: when
        }, {
            id: "1ec76026-4ecc-4d25-9c6e-cdf992a05da3",
            name: "ilastik Pixel Classifier Test",
            description: "Calls ilastik with test project.",
            script: "pixel_shell.sh",
            interpreter: "none",
            script_args: JSON.stringify({
                arguments: [{
                    value: "test/pixel_classifier_test",
                    type: TaskArgumentType.Literal
                }]
            }),
            cluster_args: JSON.stringify({arguments: [""]}),
            expected_exit_code: 0,
            work_units: 4,
            cluster_work_units: 1,
            log_prefix: "ax",
            task_repository_id: "f22c6e43-782c-4e0e-b0ca-b34fcec3340a",
            created_at: when
        }, {
            id: "a9f21399-07c0-425c-86f6-6e4f45bb06b9",
            name: "dogDescriptor",
            description: "",
            script: "dogDescriptor.sh",
            interpreter: "none",
            script_args: JSON.stringify({
                arguments: [{
                    value: "/Volumes/Spare/Projects/MouseLight/Apps/Pipeline/dogDescriptor",
                    type: TaskArgumentType.Literal
                }]
            }),
            cluster_args: JSON.stringify({arguments: [""]}),
            expected_exit_code: 0,
            work_units: 2,
            cluster_work_units: 1,
            log_prefix: "dd",
            task_repository_id: "04dbaad7-9e59-4d9e-b7b7-ae3cd1248ef9",
            created_at: when
        }, {
            id: "3ba41d1c-13d0-4def-9b5b-54d940a0fa08",
            name: "getDescriptorPerTile",
            description: "",
            script: "getDescriptorPerTile.sh",
            interpreter: "none",
            script_args: JSON.stringify({
                arguments: [{
                    value: "/Volumes/Spare/Projects/MouseLight/Apps/Pipeline/dogDescriptor/getDescriptorPerTile",
                    type: TaskArgumentType.Literal
                }]
            }),
            cluster_args: JSON.stringify({arguments: [""]}),
            expected_exit_code: 0,
            work_units: 1,
            cluster_work_units: 1,
            log_prefix: "pm",
            task_repository_id: "04dbaad7-9e59-4d9e-b7b7-ae3cd1248ef9",
            created_at: when
        }];
    }
}

function createProjects(when: Date) {
    if (isProduction) {
        return [{
            id: "44e49773-1c19-494b-b283-54466b94b70f",
            name: "Sample Brain",
            description: "Sample brain pipeline project",
            root_path: "/groups/mousebrainmicro/mousebrainmicro/data/2017-10-31/Tiling",
            log_root_path: "",
            sample_number: 99998,
            region_x_min: null,
            region_x_max: null,
            region_y_min: null,
            region_y_max: null,
            region_z_min: null,
            region_z_max: null,
            is_processing: false,
            created_at: when
        }];
    } else {
        return [{
            id: "af8cb0d4-56c0-4db8-8a1b-7b39540b2d04",
            name: "Small",
            description: "Small dashboard.json test project",
            root_path: "/Volumes/Spare/Projects/MouseLight/Dashboard Output/small",
            log_root_path: "",
            sample_number: 99998,
            region_x_min: null,
            region_x_max: null,
            region_y_min: null,
            region_y_max: null,
            region_z_min: null,
            region_z_max: null,
            is_processing: false,
            created_at: when
        }, {
            id: "f106e72c-a43e-4baf-a6f0-2395a22a65c6",
            name: "Small SubGrid",
            description: "Small dashboard.json test project",
            root_path: "/Volumes/Spare/Projects/MouseLight/Dashboard Output/small",
            log_root_path: "",
            sample_number: 99998,
            region_x_min: 1,
            region_x_max: 2,
            region_y_min: 0,
            region_y_max: 3,
            region_z_min: 2,
            region_z_max: null,
            is_processing: false,
            created_at: when
        }, {
            id: "b7b7952c-a830-4237-a3de-dcd2a388a04a",
            name: "Large",
            description: "Large dashboard.json test project",
            root_path: "/Volumes/Spare/Projects/MouseLight/Dashboard Output/large",
            log_root_path: "",
            sample_number: 99999,
            region_x_min: null,
            region_x_max: null,
            region_y_min: null,
            region_y_max: null,
            region_z_min: null,
            region_z_max: null,
            is_processing: false,
            created_at: when
        }];
    }
}

function createPipelineStages(when: Date) {
    if (isProduction) {
        return [{
            id: "90e86015-65c9-44b9-926d-deaced40ddaa",
            name: "Line Fix",
            description: "Line Fix",
            dst_path: "/nrs/mouselight/pipeline_output/2016-10-31-jan-demo/stage_1_line_fix_output",
            function_type: 2,
            is_processing: false,
            depth: 1,
            project_id: "44e49773-1c19-494b-b283-54466b94b70f",
            task_id: "ae111b6e-2187-4e07-8ccf-bc7d425d95af",
            previous_stage_id: null,
            created_at: when
        }, {
            id: "828276a5-44c0-4bd1-87f7-9495bc3e9f6c",
            name: "Classifier",
            description: "Classifier",
            dst_path: "/nrs/mouselight/pipeline_output/2016-10-31-jan-demo/stage_2_classifier_output",
            function_type: 2,
            is_processing: false,
            depth: 2,
            project_id: "44e49773-1c19-494b-b283-54466b94b70f",
            task_id: "1161f8e6-29d5-44b0-b6a9-8d3e54d23292",
            previous_stage_id: "90e86015-65c9-44b9-926d-deaced40ddaa",
            created_at: when
        }, {
            id: "5188b927-4c50-4f97-b22b-b123da78dad6",
            name: "Descriptors",
            description: "Descriptors",
            dst_path: "/nrs/mouselight/pipeline_output/2016-10-31-jan-demo/stage_3_descriptor_output",
            function_type: 2,
            is_processing: false,
            depth: 3,
            project_id: "44e49773-1c19-494b-b283-54466b94b70f",
            task_id: "a9f21399-07c0-425c-86f6-6e4f45bb06b9",
            previous_stage_id: "828276a5-44c0-4bd1-87f7-9495bc3e9f6c",
            created_at: when
        }, {
            id: "2683ad99-e389-41fd-a54c-38834ccc7ae9",
            name: "Point Match",
            description: "Point Match",
            dst_path: "/nrs/mouselight/pipeline_output/2016-10-31-jan-demo/stage_4_point_match_output",
            function_type: 5,
            is_processing: false,
            depth: 4,
            project_id: "44e49773-1c19-494b-b283-54466b94b70f",
            task_id: "3ba41d1c-13d0-4def-9b5b-54d940a0fa08",
            previous_stage_id: "5188b927-4c50-4f97-b22b-b123da78dad6",
            created_at: when
        }];
    } else {
        return [{
            id: "828276a5-44c0-4bd1-87f7-9495bc3e9f6c",
            name: "Classifier",
            description: "Classifier",
            dst_path: "/Volumes/Spare/Projects/MouseLight/PipelineOutput1",
            function_type: 2,
            is_processing: false,
            depth: 1,
            project_id: "af8cb0d4-56c0-4db8-8a1b-7b39540b2d04",
            task_id: "1ec76026-4ecc-4d25-9c6e-cdf992a05da3",
            previous_stage_id: null,
            created_at: when
        }, {
            id: "5188b927-4c50-4f97-b22b-b123da78dad6",
            name: "Descriptors",
            description: "Descriptors",
            dst_path: "/Volumes/Spare/Projects/MouseLight/PipelineOutput2",
            function_type: 2,
            is_processing: false,
            depth: 2,
            project_id: "af8cb0d4-56c0-4db8-8a1b-7b39540b2d04",
            task_id: "a9f21399-07c0-425c-86f6-6e4f45bb06b9",
            previous_stage_id: "828276a5-44c0-4bd1-87f7-9495bc3e9f6c",
            created_at: when
        }, {
            id: "2683ad99-e389-41fd-a54c-38834ccc7ae9",
            name: "Merge Descriptors",
            description: "Descriptor Merge",
            dst_path: "/Volumes/Spare/Projects/MouseLight/PipelineOutput3",
            function_type: 5,
            is_processing: false,
            depth: 3,
            project_id: "af8cb0d4-56c0-4db8-8a1b-7b39540b2d04",
            task_id: "3ba41d1c-13d0-4def-9b5b-54d940a0fa08",
            previous_stage_id: "5188b927-4c50-4f97-b22b-b123da78dad6",
            created_at: when
        }];
    }
}

function createPipelineStageFunction(when: Date) {
    return [{
        id: 1,
        name: "Refresh Dashboard Project",
        created_at: when
    }, {
        id: 2,
        name: "Map Tile",
        created_at: when
    }, {
        id: 3,
        name: "Map With X Index - 1 Tile",
        created_at: when
    }, {
        id: 4,
        name: "Map With Y Index - 1 Tile",
        created_at: when
    }, {
        id: 5,
        name: "Map With Z Index - 1 Tile",
        created_at: when
    }];
}