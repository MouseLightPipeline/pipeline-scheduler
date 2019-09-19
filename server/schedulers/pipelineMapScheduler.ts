import * as _ from "lodash";
const {performance} = require("perf_hooks");

const debug = require("debug")("pipeline:scheduler:map-scheduler");

import {IPipelineStage} from "../data-model/sequelize/pipelineStage";

import {StagePipelineScheduler} from "./stagePipelineScheduler";
import {IPipelineTile} from "../data-access/sequelize/project-connectors/stageTableConnector";
import {DefaultPipelineIdKey, IMuxTileLists, TilePipelineStatus} from "./basePipelineScheduler";
import {IProject} from "../data-model/sequelize/project";

export class PipelineMapScheduler extends StagePipelineScheduler {

    public constructor(pipelineStage: IPipelineStage, project: IProject) {
        super(pipelineStage, project);
    }

    protected async muxInputOutputTiles(project: IProject, knownInput: IPipelineTile[], knownOutput: IPipelineTile[]): Promise<IMuxTileLists> {
        let sorted: IMuxTileLists = {
            toInsert: [],
            toUpdate: [],
            toReset: [],
            toDelete: []
        };

        const toInsert = _.differenceBy(knownInput, knownOutput, DefaultPipelineIdKey);

        const toUpdate = _.intersectionBy(knownInput, knownOutput, DefaultPipelineIdKey);

        sorted.toDelete = _.differenceBy(knownOutput, knownInput, DefaultPipelineIdKey).map(t => t.relative_path);

        sorted.toInsert = toInsert.map(inputTile => {
            const now = new Date();

            return {
                relative_path: inputTile.relative_path,
                index: inputTile.index,
                tile_name: inputTile.tile_name,
                prev_stage_status: inputTile.this_stage_status,
                this_stage_status: TilePipelineStatus.Incomplete,
                lat_x: inputTile.lat_x,
                lat_y: inputTile.lat_y,
                lat_z: inputTile.lat_z,
                step_x: inputTile.step_x,
                step_y: inputTile.step_y,
                step_z: inputTile.step_z,
                // duration: 0,
                // cpu_high: 0,
                // memory_high: 0,
                created_at: now,
                updated_at: now
            };
        });

        const existingTilePaths = knownOutput.reduce((p, t) => {
            p[t.relative_path] = t;
            return p;
        }, {});

        sorted.toUpdate = toUpdate.map(inputTile => {
            const existingTile = existingTilePaths[inputTile.relative_path];

            if (existingTile === null) {
                debug(`unexpected missing tile ${inputTile.relative_path}`);
                return null;
            }

            if (existingTile.prev_stage_status !== inputTile.this_stage_status || existingTile.lat_z !== inputTile.lat_z || existingTile.step_z !== inputTile.step_z) {
                if (existingTile.this_stage_status === TilePipelineStatus.Queued && inputTile.this_stage_status !== TilePipelineStatus.Complete) {
                    sorted.toReset.push(existingTile);
                }

                existingTile.tile_name = inputTile.tile_name;
                existingTile.index = inputTile.index;
                existingTile.prev_stage_status = inputTile.this_stage_status;
                existingTile.lat_x = inputTile.lat_x;
                existingTile.lat_y = inputTile.lat_y;
                existingTile.lat_z = inputTile.lat_z;
                existingTile.step_x = inputTile.step_x;
                existingTile.step_y = inputTile.step_y;
                existingTile.step_z = inputTile.step_z;
                existingTile.updated_at = new Date();

                return existingTile;
            } else {
                return null;
            }
        }).filter(t => t !== null);

        return sorted;
    }
}
