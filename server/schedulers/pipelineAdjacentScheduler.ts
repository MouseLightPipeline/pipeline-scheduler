import * as _ from "lodash";

import {
    TilePipelineStatus,
    IMuxTileLists
} from "./basePipelineScheduler";
import {StagePipelineScheduler} from "./stagePipelineScheduler";
import {Project} from "../data-model/system/project";
import {PipelineStage, PipelineStageMethod} from "../data-model/system/pipelineStage";
import {TaskDefinition} from "../data-model/system/taskDefinition";
import {TaskExecution} from "../data-model/activity/taskExecution";
import {PipelineWorker} from "../data-model/system/pipelineWorker";
import {PipelineTile} from "../data-model/activity/pipelineTile";
import {AdjacentTile, IAdjacentTile} from "../data-model/activity/adjacentTileMap";


interface IMuxUpdateLists extends IMuxTileLists {
    toInsertAdjacentMapIndex: IAdjacentTile[];
    toDeleteAdjacentMapIndex: string[];
}

export class PipelineAdjacentScheduler extends StagePipelineScheduler {

    private _adjacentTileDelta: number = 1;

    public constructor(pipelineStage: PipelineStage, project: Project) {
        super(pipelineStage, project);
    }

    protected async getTaskContext(tile: PipelineTile): Promise<AdjacentTile> {
        return this._outputStageConnector.loadAdjacentTile(tile.id);
    }

    protected mapTaskArgumentParameter(project: Project, valueLowerCase: string, task: TaskDefinition, taskExecution: TaskExecution, worker: PipelineWorker, tile: PipelineTile, context: AdjacentTile): string {
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

    private async findAdjacentLayerTile(project: Project, inputTile: PipelineTile): Promise<PipelineTile> {
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
                let zAdjacent = inputTile.lat_z + this._adjacentTileDelta;

                const zPlaneSkip = project.zPlaneSkipIndices;

                if (zPlaneSkip.length > 0) {
                    while (_.includes(zPlaneSkip, zAdjacent)) {
                        zAdjacent++;
                    }
                }

                where = {
                    lat_x: inputTile.lat_x,
                    lat_y: inputTile.lat_y,
                    lat_z: zAdjacent
                };
                break;
        }

        return where ? await this._outputStageConnector.loadInputTile(where) : null;
    }

    protected async muxInputOutputTiles(project: Project, knownInput: PipelineTile[], knownOutput: PipelineTile[]) {
        const muxUpdateLists: IMuxUpdateLists = {
            toInsert: [],
            toUpdate: [],
            toReset: [],
            toDelete: [],
            toInsertAdjacentMapIndex: [],
            toDeleteAdjacentMapIndex: []
        };

        // Flatten input and and output for faster searching.
        const knownOutputIdLookup = knownOutput.reduce((p, t) => {
            p[t.relative_path] = t;
            return p;
        }, {});
        const knownInputIdLookup = knownInput.reduce((p, t) => {
            p[t.relative_path] = t;
            return p;
        }, {});

        // List of tiles where we already know the previous layer tile id.
        const adjacentMapRows = await this._outputStageConnector.loadAdjacentTiles();
        const adjacentMapIdLookup = adjacentMapRows.reduce((p, t) => {
            p[t.relative_path] = t;
            return p;
        }, {});

        muxUpdateLists.toDelete = _.differenceBy(knownOutput, knownInput, "relative_path").map(t => t.relative_path);

        // Force serial execution of each tile given async calls within function.
        await knownInput.reduce(async (promiseChain, inputTile) => {
            return promiseChain.then(() => {
                return this.updateAdjacentTile(project, inputTile, knownInputIdLookup, knownOutputIdLookup, adjacentMapIdLookup, muxUpdateLists.toDelete, muxUpdateLists);
            });
        }, Promise.resolve());

        await this._outputStageConnector.insertAdjacent(muxUpdateLists.toInsertAdjacentMapIndex);

        await this._outputStageConnector.deleteAdjacent(muxUpdateLists.toDeleteAdjacentMapIndex);

        // Insert, update, delete handled by base.
        return muxUpdateLists;
    }

    private async updateAdjacentTile(project: Project, inputTile: PipelineTile, knownInputIdLookup, knownOutputIdLookup, nextLayerMapIdLookup, toDelete: string[], muxUpdateLists: IMuxUpdateLists): Promise<void> {
        // If the source tile is now in a skip plane, remove and do not remap.
        if (_.includes(project.zPlaneSkipIndices, inputTile.lat_z) || _.includes(toDelete, inputTile.relative_path)) {
            muxUpdateLists.toDeleteAdjacentMapIndex.push(inputTile.relative_path);
            return;
        }

        const existingOutput = knownOutputIdLookup[inputTile.relative_path] || null;

        let adjacentMap = nextLayerMapIdLookup[inputTile.relative_path] || null;

        const tile = await this.findAdjacentLayerTile(project, inputTile);

        if (adjacentMap !== null) {
            // Assert the existing map is still valid given something is curated/deleted.  Either the existing listed
            // adjacent tile itself could have been deleted or which tile is considered adjacent may have changed.  For
            // example, the layer has been added to skip planes and there is no match in the next plane (tile == null)
            const isDeleted = toDelete.indexOf(adjacentMap.adjacent_relative_path) >= 0 || tile == null;

            if (isDeleted || tile == null) {
                // Remove entry.  If a replacement exists, will be captured next time around where adjacentMap will then
                // come back as null.
                muxUpdateLists.toDeleteAdjacentMapIndex.push(inputTile.relative_path);
            } else if (adjacentMap.lat_z !== tile.lat_z || adjacentMap.adjacent_relative_path !== adjacentMap.adjacent_relative_path) {
                // findAdjacentLayerTile() returned with a different tile or a modified depth for the tile.
                await adjacentMap.update({
                    adjacent_relative_path: tile.relative_path,
                    adjacent_tile_name: tile.tile_name,
                    lat_x: tile.lat_x,
                    lat_y: tile.lat_y,
                    lat_z: tile.lat_z
                });
            }
        } else if (tile !== null) {
            adjacentMap = {
                relative_path: inputTile.relative_path,
                adjacent_relative_path: tile.relative_path,
                adjacent_tile_name: tile.tile_name,
                lat_x: tile.lat_x,
                lat_y: tile.lat_y,
                lat_z: tile.lat_z
            };

            muxUpdateLists.toInsertAdjacentMapIndex.push(adjacentMap);
        }

        // This really shouldn't fail since we should have already seen the tile at some point to have created the
        // mapping.
        const adjacentInputTile = adjacentMap ? knownInputIdLookup[adjacentMap.adjacent_relative_path] || null : null;

        let prev_status = TilePipelineStatus.DoesNotExist;

        let this_status = TilePipelineStatus.Incomplete;

        // We can only be in this block if the adjacent tile exists.  If the adjacent tile does not exist, the tile
        // effectively does not exist for this stage.
        if (adjacentInputTile !== null) {
            if ((inputTile.stage_status === TilePipelineStatus.Failed) || (adjacentInputTile.stage_status === TilePipelineStatus.Failed)) {
                prev_status = TilePipelineStatus.Failed;
            } else if ((inputTile.stage_status === TilePipelineStatus.Canceled) || (adjacentInputTile.stage_status === TilePipelineStatus.Canceled)) {
                prev_status = TilePipelineStatus.Canceled;
            } else {
                // This works because once you drop failed and canceled, the highest value is complete.
                prev_status = Math.min(inputTile.stage_status, adjacentInputTile.stage_status);
            }
        } else {
            this_status = TilePipelineStatus.DoesNotExist;
        }

        if (existingOutput) {
            // If the previous stage is in the middle of processing, maintain the current status - nothing has
            // changed (we don't kill a running task because a tile has been curated - it will be removed when done).
            // Otherwise. something reset on the last stage tile and need to go back to incomplete.
            if (existingOutput.stage_status !== TilePipelineStatus.Processing) {
                // In all cases but the above, if this has been marked does not exist above due to previous stage info
                // that is the final answer.
                if (this_status !== TilePipelineStatus.DoesNotExist) {
                    if ((prev_status !== TilePipelineStatus.DoesNotExist) && (existingOutput.stage_status === TilePipelineStatus.DoesNotExist)) {
                        // It was considered does not exist (maybe the adjacent tile had not been acquired yet), but now there is a
                        // legit value for the previous stage, so upgrade to incomplete.
                        this_status = TilePipelineStatus.Incomplete;
                    } else if (inputTile.stage_status !== TilePipelineStatus.Complete) {
                        // If this is a regression in the previous stage, this needs to be reverted to incomplete.
                        this_status = TilePipelineStatus.Incomplete;
                    } else {
                        // Otherwise no change.
                        this_status = existingOutput.stage_status;
                    }
                } else {
                }
            } else {
                this_status = existingOutput.tstage_status;
            }

            if (existingOutput.stage_status !== this_status || existingOutput.lat_z !== inputTile.lat_z || existingOutput.step_z !== inputTile.step_z) {
                if (existingOutput.stage_status === TilePipelineStatus.Queued && inputTile.stage_status !== TilePipelineStatus.Complete) {
                    muxUpdateLists.toReset.push(existingOutput);
                }

                existingOutput.index = inputTile.index;
                existingOutput.tile_name = inputTile.tile_name;
                existingOutput.stage_status = this_status;
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
            muxUpdateLists.toInsert.push({
                    stage_id: this._sourceId,
                    relative_path: inputTile.relative_path,
                    index: inputTile.index,
                    tile_name: inputTile.tile_name,
                    stage_status: this_status,
                    lat_x: inputTile.lat_x,
                    lat_y: inputTile.lat_y,
                    lat_z: inputTile.lat_z,
                    step_x: inputTile.step_x,
                    step_y: inputTile.step_y,
                    step_z: inputTile.step_z
                }
            );
        }
    }
}
