import * as _ from "lodash";

import {
    TilePipelineStatus,
    DefaultPipelineIdKey,
    IMuxTileLists
} from "./basePipelineScheduler";
import {IPipelineStage, PipelineStageMethod} from "../data-model/sequelize/pipelineStage";
import {IPipelineTile, IPipelineTileAttributes} from "../data-access/sequelize/project-connectors/stageTableConnector";
import {StagePipelineScheduler} from "./stagePipelineScheduler";
import {
    AdjacentTileStageConnector, IAdjacentTile,
    IAdjacentTileAttributes
} from "../data-access/sequelize/project-connectors/adjacentTileStageConnector";
import {IProject} from "../data-model/sequelize/project";
import {IPipelineWorker} from "../data-model/sequelize/pipelineWorker";
import {ITaskDefinition} from "../data-model/sequelize/taskDefinition";
import {ITaskExecutionAttributes} from "../data-model/taskExecution";

interface IMuxUpdateLists extends IMuxTileLists {
    toInsertAdjacentMapIndex: IAdjacentTileAttributes[];
    toDeleteAdjacentMapIndex: string[];
}

export class PipelineAdjacentScheduler extends StagePipelineScheduler {

    private _adjacentTileDelta: number = 1;

    public constructor(pipelineStage: IPipelineStage, project: IProject) {
        super(pipelineStage, project);
    }

    public set AdjacentTileDelta(delta: number) {
        this._adjacentTileDelta = delta;
    }

    public get OutputStageConnector(): AdjacentTileStageConnector {
        return this._outputStageConnector as AdjacentTileStageConnector;
    }

    protected async getTaskContext(tile: IPipelineTileAttributes): Promise<IAdjacentTile> {
        return this.OutputStageConnector.loadAdjacentTile(tile.relative_path);
    }

    protected mapTaskArgumentParameter(project: IProject, valueLowerCase: string, task: ITaskDefinition, taskExecution: ITaskExecutionAttributes, worker: IPipelineWorker, tile: IPipelineTileAttributes, context: IAdjacentTile): string {
        if (context !== null) {
            switch (valueLowerCase) {
                case "adjacent_tile_relative_path":
                    return context.adjacent_relative_path;
                case "adjacent_tile_name":
                    return context.adjacent_tile_name;
            }
        }

        return super.mapTaskArgumentParameter(project, valueLowerCase, task, taskExecution, worker, tile, context);
    }

    private async findAdjacentLayerTile(inputTile: IPipelineTileAttributes): Promise<IPipelineTile> {
        let where = null;

        switch (this._pipelineStage.function_type) {
            case PipelineStageMethod.XAdjacentTileComparison:
                where = {
                    lat_x: inputTile.lat_x + this._adjacentTileDelta,
                    lat_y: inputTile.lat_y,
                    lat_z: inputTile.lat_z
                };
                break;
            case PipelineStageMethod.YAdjacentTileComparison:
                where = {
                    lat_x: inputTile.lat_x,
                    lat_y: inputTile.lat_y + this._adjacentTileDelta,
                    lat_z: inputTile.lat_z
                };
                break;
            case PipelineStageMethod.ZAdjacentTileComparison:
                where = {
                    lat_x: inputTile.lat_x,
                    lat_y: inputTile.lat_y,
                    lat_z: inputTile.lat_z + this._adjacentTileDelta
                };
                break;
        }

        return where ? await this._inputStageConnector.loadTile(where) : null;
    }

    protected async muxInputOutputTiles(knownInput: IPipelineTile[], knownOutput: IPipelineTile[]) {
        const muxUpdateLists: IMuxUpdateLists = {
            toInsert: [],
            toUpdate: [],
            toReset: [],
            toDelete: [],
            toInsertAdjacentMapIndex: [],
            toDeleteAdjacentMapIndex: []
        };

        // Flatten input and and output for faster searching.
        // const knownOutputIdLookup = knownOutput.map(obj => obj[DefaultPipelineIdKey]);
        // const knownInputIdLookup = knownInput.map(obj => obj[DefaultPipelineIdKey]);
        const knownOutputIdLookup = knownOutput.reduce((p, t) => {
            p[t.relative_path] = t;
            return p;
        }, {});
        const knownInputIdLookup = knownInput.reduce((p, t) => {
            p[t.relative_path] = t;
            return p;
        }, {});

        // List of tiles where we already know the previous layer tile id.
        // const adjacentMapRows = await this.zIndexMapTable.select();
        const adjacentMapRows = await this.OutputStageConnector.loadAdjacentTiles();
        // const adjacentMapIdLookup = adjacentMapRows.map(obj => obj[DefaultPipelineIdKey]);
        const adjacentMapIdLookup = adjacentMapRows.reduce((p, t) => {
            p[t.relative_path] = t;
            return p;
        }, {});

        muxUpdateLists.toDelete = _.differenceBy(knownOutput, knownInput, DefaultPipelineIdKey).map(t => t.relative_path);

        // Force serial execution of each tile given async calls within function.
        await knownInput.reduce(async (promiseChain, inputTile) => {
            return promiseChain.then(() => {
                return this.muxUpdateTile(inputTile, knownInputIdLookup, knownOutputIdLookup, adjacentMapIdLookup, muxUpdateLists.toDelete, muxUpdateLists);
            });
        }, Promise.resolve());

        await this.OutputStageConnector.insertAdjacent(muxUpdateLists.toInsertAdjacentMapIndex);

        await this.OutputStageConnector.deleteAdjacent(muxUpdateLists.toDeleteAdjacentMapIndex);

        // Insert, update, delete handled by base.
        return muxUpdateLists;
    }

    private async muxUpdateTile(inputTile: IPipelineTile, knownInputIdLookup, knownOutputIdLookup, nextLayerMapIdLookup, toDelete: string[], muxUpdateLists: IMuxUpdateLists): Promise<void> {
        // const idx = knownOutputIdLookup.indexOf(inputTile[DefaultPipelineIdKey]);

        // const existingOutput: IPipelineTile = idx > -1 ? knownOutput[idx] : null;

        const existingOutput = knownOutputIdLookup[inputTile.relative_path] || null;

        // const adjacentLookupIndex = nextLayerMapIdLookup.indexOf(inputTile[DefaultPipelineIdKey]);

        // let adjacentMap: IAdjacentTileAttributes = adjacentLookupIndex > -1 ? nextLayerMapRows[adjacentLookupIndex] : null;

        let adjacentMap: IAdjacentTileAttributes = nextLayerMapIdLookup[inputTile.relative_path] || null;

        let tile = null;

        if (adjacentMap === null) {
            tile = await this.findAdjacentLayerTile(inputTile);
        } else {
            // Assert the existing map is still valid given something is curated/deleted.
            const index = toDelete.indexOf(adjacentMap.adjacent_relative_path);

            // Remove entry.  If a replacement exists, will be captured next time around.
            if (index >= 0) {
                muxUpdateLists.toDeleteAdjacentMapIndex.push(inputTile.relative_path);
            }
        }

        if (tile !== null) {
            adjacentMap = {
                relative_path: inputTile.relative_path,
                adjacent_relative_path: tile.relative_path,
                adjacent_tile_name: tile.tile_name
            };

            muxUpdateLists.toInsertAdjacentMapIndex.push(adjacentMap);
        }

        // This really shouldn't fail since we should have already seen the tile at some point to have created the
        // mapping.
        // const adjacentInputTileIdx = adjacentMap ? knownInputIdLookup.indexOf(adjacentMap.adjacent_relative_path) : -1;
        // const adjacentInputTile = adjacentInputTileIdx > -1 ? knownInput[adjacentInputTileIdx] : null;
        const adjacentInputTile = adjacentMap ? knownInputIdLookup[adjacentMap.adjacent_relative_path] || null : null;

        let prev_status = TilePipelineStatus.DoesNotExist;

        let this_status = TilePipelineStatus.Incomplete;

        // We can only be in this block if the adjacent tile exists.  If the adjacent tile does not exist, the tile
        // effectively does not exist for this stage.
        if (adjacentInputTile !== null) {
            if ((inputTile.this_stage_status === TilePipelineStatus.Failed) || (adjacentInputTile.this_stage_status === TilePipelineStatus.Failed)) {
                prev_status = TilePipelineStatus.Failed;
            } else if ((inputTile.this_stage_status === TilePipelineStatus.Canceled) || (adjacentInputTile.this_stage_status === TilePipelineStatus.Canceled)) {
                prev_status = TilePipelineStatus.Canceled;
            } else {
                // This works because once you drop failed and canceled, the highest value is complete.
                prev_status = Math.min(inputTile.this_stage_status, adjacentInputTile.this_stage_status);
            }
        } else {
            this_status = TilePipelineStatus.DoesNotExist;
        }

        if (existingOutput) {
            // If the previous stage is in the middle of processing, maintain the current status - nothing has
            // changed (we don't kill a running task because a tile has been curated - it will be removed when done).
            // Otherwise. something reset on the last stage tile and need to go back to incomplete.
            if (existingOutput.this_stage_status !== TilePipelineStatus.Processing) {
                // In all cases but the above, if this has been marked does not exist above due to previous stage info
                // that is the final answer.
                if (this_status !== TilePipelineStatus.DoesNotExist) {
                    if ((prev_status !== TilePipelineStatus.DoesNotExist) && (existingOutput.this_stage_status === TilePipelineStatus.DoesNotExist)) {
                        // It was considered does not exist (maybe the adjacent tile had not been acquired yet), but now there is a
                        // legit value for the previous stage, so upgrade to incomplete.
                        this_status = TilePipelineStatus.Incomplete;
                    } else if (inputTile.this_stage_status !== TilePipelineStatus.Complete) {
                        // If this is a regression in the previous stage, this needs to be reverted to incomplete.
                        this_status = TilePipelineStatus.Incomplete;
                    } else {
                        // Otherwise no change.
                        this_status = existingOutput.this_stage_status;
                    }
                } else {
                }
            } else {
                this_status = existingOutput.this_stage_status;
            }

            if (existingOutput.prev_stage_status !== prev_status || existingOutput.this_stage_status !== this_status || existingOutput.lat_z !== inputTile.lat_z || existingOutput.step_z !== inputTile.step_z) {
                if (existingOutput.this_stage_status === TilePipelineStatus.Queued && inputTile.this_stage_status !== TilePipelineStatus.Complete) {
                    muxUpdateLists.toReset.push(existingOutput);
                }

                existingOutput.index = inputTile.index;
                existingOutput.tile_name = inputTile.tile_name;
                existingOutput.prev_stage_status = prev_status;
                existingOutput.this_stage_status = this_status;
                existingOutput.lat_x = inputTile.lat_x;
                existingOutput.lat_y = inputTile.lat_y;
                existingOutput.lat_z = inputTile.lat_z;
                existingOutput.step_x = inputTile.step_x;
                existingOutput.step_y = inputTile.step_y;
                existingOutput.step_z = inputTile.step_z;
                existingOutput.updated_at = new Date();

                muxUpdateLists.toUpdate.push(existingOutput);
            }
        } else {
            let now = new Date();

            muxUpdateLists.toInsert.push({
                    relative_path: inputTile.relative_path,
                    index: inputTile.index,
                    tile_name: inputTile.tile_name,
                    prev_stage_status: prev_status,
                    this_stage_status: this_status,
                    lat_x: inputTile.lat_x,
                    lat_y: inputTile.lat_y,
                    lat_z: inputTile.lat_z,
                    step_x: inputTile.step_x,
                    step_y: inputTile.step_y,
                    step_z: inputTile.step_z,
                    created_at: now,
                    updated_at: now
                }
            );
        }
    }
}
