import { logger } from "@project-sunbird/ext-framework-server/logger";
import { Manifest } from "@project-sunbird/ext-framework-server/models";
import * as childProcess from "child_process";
import * as _ from "lodash";
import { containerAPI } from "OpenRAP/dist/api";
import * as path from "path";
import * as TreeModel from "tree-model";
import { Inject } from "typescript-ioc";
import DatabaseSDK from "../../sdk/database";
import Response from "../../utils/response";
import Content from "./content";
import { IContentDelete, IDeletePath } from './IContent';

export default class ContentDelete {
    private workerProcessRef: childProcess.ChildProcess;
    @Inject
    private databaseSdk: DatabaseSDK;
    private fileSDK: any;
    private content: Content;
    constructor(manifest: Manifest) {
        this.content = new Content(manifest);
        this.databaseSdk.initialize(manifest.id);
        this.fileSDK = containerAPI.getFileSDKInstance(manifest.id);
    }

    public async delete(req, res) {
        const reqId = req.headers["X-msgid"];
        logger.debug(`${reqId}: Delete method is called`);
        const contentIDS: string[] = _.get(req.body, "request.contents");
        if (!contentIDS) {
            logger.error(`${reqId}: Error: content Ids not found`);
            return res.status(400).send(Response.error(`api.content.delete`, 400, "MISSING_CONTENTS"));
        }
        try {
            if (!this.workerProcessRef) {
                this.workerProcessRef = childProcess.fork(path.join(__dirname, "contentDeleteHelper"));
            }
            const failed: object[] = [];
            const visibility = _.get(req.body, "request.visibility");
            let contentsToDelete = await this.content.searchInDB({ identifier: contentIDS },
                req.headers["X-msgid"], "", visibility);
            contentsToDelete = await this.getContentsToDelete(contentsToDelete.docs);
            let deleted = await this.databaseSdk.bulk("content", contentsToDelete).catch((err) => {
                    failed.push(err.message || err.errMessage);
            });
            deleted =  _.map(deleted, (content) => content.id);
            const contentPaths: IDeletePath[] = _.map(deleted, (id) => {
                if (id) {
                    return ({ path: path.join("content", id) });
                }
            });
            if (contentPaths) {
                this.workerProcessRef.send(contentPaths);
            }
            res.send(Response.success("api.content.delete", {deleted, failed}, req));
            } catch (err) {
                logger.error(`Received Error while Deleting content `, err);
                res.status(500);
                res.send(Response.error(`api.content.delete`, 500, err.errMessage || err.message, err.code));
            }
    }

    public async getContentsToDelete(contentsToDelete: IContentDelete[]): Promise <IContentDelete[]> {
        logger.debug(`getContentsToDelete() is called`);
        const deleteContents: IContentDelete[] = [];
        for (const content of contentsToDelete) {
            content.desktopAppMetadata.isAvailable = false;
            deleteContents.push(content);
            if (content.mimeType === "application/vnd.ekstep.content-collection") {
                const children: object[] = await this.getResources(content);
                for (const child of children["docs"]) {
                    child.desktopAppMetadata.isAvailable = false;
                    deleteContents.push(child);
                }
            }
        }
        return deleteContents;
    }

    public async getResources(content: {}): Promise<object[]> {
        logger.debug(`getResources() is called`);
        const resourceIds: string[] = [];
        const model = new TreeModel();
        let treeModel;
        treeModel = model.parse(content);
        treeModel.walk(node => {
            if (node.model.mimeType !== 'application/vnd.ekstep.content-collection') {
                resourceIds.push(node.model.identifier);
            }
        });
        const dbFilter = {
            selector: {
                $and: [
                    {
                        _id: {
                            $in: resourceIds,
                        },
                    },
                    {
                        mimeType: {
                            $nin: ["application/vnd.ekstep.content-collection"],
                        },
                    },
                    {
                        visibility: {
                            $eq: "Parent",
                        },
                    },
                ],
            },
        };
        logger.info(`finding all child contents of a collection`);
        return await this.databaseSdk.find("content", dbFilter);
    }

    public async reconciliation() {
        try {
            logger.debug(`deleteReconciliation() is called`);
            if (!this.workerProcessRef) {
                this.workerProcessRef = childProcess.fork(path.join(__dirname, "contentDeleteHelper"));
            }
            const dbFilter = {
                selector: {
                    "desktopAppMetadata.isAvailable": false,
                },
            };
            logger.info(`finding all contentsToDelete in Queue `);
            const contents = await this.databaseSdk.find("content", dbFilter).catch((error) => {
                logger.error(`Received Error while finding contents (isAvailable : false) Error: ${error.stack}`);
            });
            const contentsToDelete: IContentDelete[] = await this.getContentsToDelete(contents.docs);
            const deleted = await this.databaseSdk.bulk("content", contentsToDelete);
            const contentPaths: IDeletePath[] = _.map(deleted, (content) => {
                if (!_.isEmpty(_.get(content, "id"))) {
                    return ({ path: path.join("content", content.id) });
                }
            });
            if (contentPaths) {
                this.workerProcessRef.send(contentPaths);
            }
        } catch (err) {
            logger.error(`Received Error While deleting contents In reconciliation() Error: ${err.stack}`);
        }
    }
}
