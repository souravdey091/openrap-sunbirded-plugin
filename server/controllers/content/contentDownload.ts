import { logger } from "@project-sunbird/ext-framework-server/logger";
import { Manifest } from "@project-sunbird/ext-framework-server/models";
import { containerAPI, ISystemQueueInstance } from "OpenRAP/dist/api";
import { HTTPService } from "@project-sunbird/ext-framework-server/services";
import * as _ from "lodash";
import * as path from "path";
import { Inject } from "typescript-ioc";
import TelemetryHelper from "../../helper/telemetryHelper";
import DatabaseSDK from "../../sdk/database";
import Response from "../../utils/response";
const sessionStartTime = Date.now();
export enum CONTENT_DOWNLOAD_STATUS {
    Submitted = "SUBMITTED",
    Completed = "COMPLETED",
    Extracted = "EXTRACTED",
    Indexed = "INDEXED",
    Failed = "FAILED",
    Paused = "PAUSED",
    Canceled = "CANCELED",
}
export enum contentZipToUnzipRatio {
    "application/vnd.ekstep.content-collection" = 1.5,
    "application/epub" = 1.5,
    "application/vnd.ekstep.html-archive" = 3,
    "video/webm" = 1.5,
    "video/mp4" = 1.5,
    "application/vnd.ekstep.h5p-archive" = 3,
    "application/pdf" = 1.5,
    "application/vnd.ekstep.ecml-archive" = 3,
    "video/x-youtube" = 1.5,
}
enum API_DOWNLOAD_STATUS {
    inprogress = "INPROGRESS",
    submitted = "SUBMITTED",
    completed = "COMPLETED",
    failed = "FAILED",
    paused = "PAUSED",
    canceled = "CANCELED",
}

const dbName = "content_download";
export default class ContentDownload {

    private contentsFilesPath: string = "content";
    private ecarsFolderPath: string = "ecars";

    @Inject
    private databaseSdk: DatabaseSDK;

    @Inject private telemetryHelper: TelemetryHelper;

    private downloadManager;
    private pluginId;
    private systemSDK;
    private systemQueue: ISystemQueueInstance;
    constructor(manifest: Manifest) {
        this.databaseSdk.initialize(manifest.id);
        this.pluginId = manifest.id;
        this.downloadManager = containerAPI.getDownloadManagerInstance(this.pluginId);
        this.systemSDK = containerAPI.getSystemSDKInstance(manifest.id);
        this.systemQueue = containerAPI.getSystemQueueInstance(manifest.id);
    }

    public download(req: any, res: any): any {
        (async () => {
            try {
                logger.debug(`ReqId = "${req.headers["X-msgid"]}": Content Download method is called`);
                // get the content using content read api
                logger.debug(`ReqId = "${req.headers["X-msgid"]}": Get the content using content read api`);
                const content = await HTTPService.get(`${process.env.APP_BASE_URL}/api/content/v1/read/${req.params.id}`, {}).toPromise();
                logger.info(`ReqId = "${req.headers["X-msgid"]}": Content: ${_.get(content, "data.result.content.identifier")} found from content read api`);
                if (_.get(content, "data.result.content.mimeType")) {
                    // Adding telemetry share event
                    this.constructShareEvent(content);
                    // check if the content is type collection
                    logger.debug(`ReqId = "${req.headers["X-msgid"]}": check if the content is of type collection`);
                    if (_.get(content, "data.result.content.mimeType") !== "application/vnd.ekstep.content-collection") {
                        logger.info(`ReqId = "${req.headers["X-msgid"]}": Found content:${_.get(content, "data.result.content.mimeType")} is not of type collection`);
                        // insert to the to content_download_queue
                        // add the content to queue using downloadManager
                        const zipSize = (_.get(content, "data.result.content.size") as number);
                        await this.checkDiskSpaceAvailability(zipSize, false);
                        const downloadFiles = [{
                            id: (_.get(content, "data.result.content.identifier") as string),
                            url: (_.get(content, "data.result.content.downloadUrl") as string),
                            size: (_.get(content, "data.result.content.size") as number),
                        }];
                        const downloadId = await this.downloadManager.download(downloadFiles, "ecars");
                        const queueMetaData = {
                            mimeType: _.get(content, "data.result.content.mimeType"),
                            items: downloadFiles,
                            pkgVersion: _.get(content, "data.result.content.pkgVersion"),
                            contentType: _.get(content, "data.result.content.contentType"),
                            resourceId: _.get(content, "data.result.content.identifier"),
                        };
                        logger.debug(`ReqId = "${req.headers["X-msgid"]}": insert to the content_download_queue`);
                        await this.databaseSdk.insert(dbName, {
                            downloadId,
                            contentId: _.get(content, "data.result.content.identifier"),
                            identifier: _.get(content, "data.result.content.identifier"),
                            name: _.get(content, "data.result.content.name"),
                            status: CONTENT_DOWNLOAD_STATUS.Submitted,
                            queueMetaData,
                            createdOn: Date.now(),
                            updatedOn: Date.now(),
                            size: (_.get(content, "data.result.content.size") as number),
                        });
                        logger.info(`ReqId = "${req.headers["X-msgid"]}": Content Inserted in Database Successfully`);
                        return res.send(Response.success("api.content.download", { downloadId }, req));
                        // return response the downloadId
                    } else {
                        logger.info(`ReqId = "${req.headers["X-msgid"]}": Found content:${_.get(content, "data.result.content.mimeType")} is of type collection`);
                        const downloadFiles = [{
                            id: (_.get(content, "data.result.content.identifier") as string),
                            url: (_.get(content, "data.result.content.downloadUrl") as string),
                            size: (_.get(content, "data.result.content.size") as number),
                        }];
                        let totalCollectionSize = _.get(content, "data.result.content.size");
                        // get the child contents
                        const childNodes = _.get(content, "data.result.content.childNodes");
                        if (!_.isEmpty(childNodes)) {
                            logger.debug(`ReqId = "${req.headers["X-msgid"]}": Get the child contents using content search API`);
                            const childrenContentsRes = await HTTPService.post(`${process.env.APP_BASE_URL}/api/content/v1/search`,
                                {
                                    request: {
                                        filters: {
                                            identifier: childNodes,
                                            mimeType: { "!=": "application/vnd.ekstep.content-collection" },
                                        },
                                        limit: childNodes.length,
                                    },
                                }, {
                                headers: {
                                    "Content-Type": "application/json",
                                },
                            }).toPromise();
                            logger.info(`ReqId = "${req.headers["X-msgid"]}": Found child contents: ${_.get(childrenContentsRes, "data.result.count")}`);
                            if (_.get(childrenContentsRes, "data.result.count")) {
                                const contents = _.get(childrenContentsRes, "data.result.content");
                                for (const content of contents) {
                                    totalCollectionSize += _.get(content, "size");
                                    downloadFiles.push({
                                        id: (_.get(content, "identifier") as string),
                                        url: (_.get(content, "downloadUrl") as string),
                                        size: (_.get(content, "size") as number),
                                    });
                                }
                            }

                        }
                        const downloadId = await this.downloadManager.download(downloadFiles, "ecars");
                        const queueMetaData = {
                            mimeType: _.get(content, "data.result.content.mimeType"),
                            items: downloadFiles,
                            pkgVersion: _.get(content, "data.result.content.pkgVersion"),
                            contentType: _.get(content, "data.result.content.contentType"),
                            resourceId: _.get(content, "data.result.content.identifier"),
                        };
                        await this.checkDiskSpaceAvailability(totalCollectionSize, true);
                        logger.debug(`ReqId = "${req.headers["X-msgid"]}": insert collection in Database`);
                        await this.databaseSdk.insert(dbName, {
                            downloadId,
                            contentId: _.get(content, "data.result.content.identifier"),
                            identifier: _.get(content, "data.result.content.identifier"),
                            name: _.get(content, "data.result.content.name"),
                            status: CONTENT_DOWNLOAD_STATUS.Submitted,
                            queueMetaData,
                            createdOn: Date.now(),
                            updatedOn: Date.now(),
                            size: totalCollectionSize,
                        });
                        logger.info(`ReqId = "${req.headers["X-msgid"]}": Collection inserted successfully`);
                        return res.send(Response.success("api.content.download", { downloadId }, req));
                    }
                } else {
                    logger.error(`ReqId = "${req.headers["X-msgid"]}": Received error while processing download request ${content}, for content ${req.params.id}`);
                    res.status(500);
                    return res.send(Response.error("api.content.download", 500));
                }

            } catch (error) {
                logger.error(`ReqId = "${req.headers["X-msgid"]}": Received error while processing download request and err.message: ${error.message}, for content ${req.params.id}`);
                if (_.get(error, "code") === "LOW_DISK_SPACE") {
                    res.status(507);
                    return res.send(Response.error("api.content.download", 507, "Low disk space", "LOW_DISK_SPACE"));
                }
                res.status(500);
                return res.send(Response.error("api.content.download", 500));
            }
        })();
    }

    public async list(req: any, res: any) {
        try {
            const activeSelector = {
                isActive: true,
                group: 'CONTENT_MANAGER',
            };
            const inActiveSelector = {
                isActive: false,
                group: 'CONTENT_MANAGER',
                updatedOn: { "$gt": sessionStartTime },
            };
            const activeDbData = await this.systemQueue.query(activeSelector);
            const inActiveDbData = await this.systemQueue.query(inActiveSelector);
            const dbData = _.concat(activeDbData.docs, inActiveDbData.docs);
            const listData = [];
            _.forEach(dbData, (data) => {
                const listObj = {
                    contentId: _.get(data, 'metaData.contentId'),
                    identifier: _.get(data, 'metaData.contentId'),
                    id: _.get(data, '_id'),
                    resourceId: _.get(data, 'metaData.contentId'),
                    name: _.get(data, 'name'),
                    totalSize: _.get(data, 'metaData.contentSize'),
                    downloadedSize: _.get(data, 'progress'),
                    status: _.get(data, 'status'),
                    createdOn: _.get(data, 'createdOn'),
                    pkgVersion: _.get(data, 'metaData.pkgVersion'),
                    mimeType: _.get(data, 'metaData.mimeType'),
                    failedCode: _.get(data, 'failedCode'),
                    failedReason: _.get(data, 'failedReason'),
                    addedUsing: _.toLower(_.get(data, 'type')),
                };
                listData.push(listObj);
            });
            return res.send(Response.success("api.content.list", {
                response: {
                    contents: _.uniqBy(_.orderBy(listData, ["createdOn"], ["desc"]), "id"),
                },
            }, req));
        } catch (error) {
            logger.error(`ReqId = "${req.headers['X-msgid']}": Error while processing the content list request and err.message: ${error.message}`);
            res.status(500);
            return res.send(Response.error("api.content.list", 500));
        }
    }

    public async pause(req: any, res: any) {
        try {
            const downloadId = _.get(req, "params.downloadId");
            await this.downloadManager.pause(downloadId);
            const dbResp = await this.databaseSdk.find(dbName, {
                selector: { downloadId },
            });

            await this.databaseSdk.update(dbName, dbResp.docs[0]._id, {
                updatedOn: Date.now(),
                status: CONTENT_DOWNLOAD_STATUS.Paused,
            });
            return res.send(Response.success("api.content.pause.download", downloadId, req));
        } catch (error) {
            logger.error(`ReqId = "${req.headers["X-msgid"]}": Received error while pausing download,  where error = ${error}`);
            const status = _.get(error, "status") || 500;
            res.status(status);
            return res.send(
                Response.error("api.content.pause.download", status, _.get(error, "message"), _.get(error, "code")),
            );
        }
    }

    public async resume(req: any, res: any) {
        try {
            const downloadId = _.get(req, "params.downloadId");
            await this.downloadManager.resume(downloadId);
            const dbResp = await this.databaseSdk.find(dbName, {
                selector: { downloadId },
            });

            await this.databaseSdk.update(dbName, dbResp.docs[0]._id, {
                updatedOn: Date.now(),
                status: CONTENT_DOWNLOAD_STATUS.Submitted,
            });
            return res.send(Response.success("api.content.resume.download", downloadId, req));
        } catch (error) {
            logger.error(`ReqId = "${req.headers["X-msgid"]}": Received error while resuming download,  where error = ${error}`);
            const status = _.get(error, "status") || 500;
            res.status(status);
            return res.send(
                Response.error("api.content.resume.download", status, _.get(error, "message"), _.get(error, "code")),
            );
        }
    }

    public async cancel(req: any, res: any) {
        try {
            const downloadId = _.get(req, "params.downloadId");
            await this.downloadManager.cancel(downloadId);
            const dbResp = await this.databaseSdk.find(dbName, {
                selector: { downloadId },
            });
            await this.databaseSdk.update(dbName, dbResp.docs[0]._id, {
                updatedOn: Date.now(),
                status: CONTENT_DOWNLOAD_STATUS.Canceled,
            });
            return res.send(Response.success("api.content.cancel.download", downloadId, req));
        } catch (error) {
            logger.error(`ReqId = "${req.headers["X-msgid"]}": Received error while canceling download,  where error = ${error}`);
            const status = _.get(error, "status") || 500;
            res.status(status);
            return res.send(
                Response.error("api.content.cancel.download", status, _.get(error, "message"), _.get(error, "code")),
            );
        }
    }

    public async retry(req: any, res: any) {
        try {
            const downloadId = _.get(req, "params.downloadId");
            await this.downloadManager.retry(downloadId);
            const dbResp = await this.databaseSdk.find(dbName, {
                selector: { downloadId },
            });

            await this.databaseSdk.update(dbName, dbResp.docs[0]._id, {
                updatedOn: Date.now(),
                status: CONTENT_DOWNLOAD_STATUS.Submitted,
            });
            return res.send(Response.success("api.content.retry.download", downloadId, req));
        } catch (error) {
            logger.error(`ReqId = "${req.headers["X-msgid"]}": Received error while retrying download,  where error = ${error}`);
            const status = _.get(error, "status") || 500;
            res.status(status);
            return res.send(
                Response.error("api.content.retry.download", status, _.get(error, "message"), _.get(error, "code")),
            );
        }
    }

    private constructShareEvent(content) {
        const telemetryShareItems = [{
            id: _.get(content, "data.result.content.identifier"),
            type: _.get(content, "data.result.content.contentType"),
            ver: _.toString(_.get(content, "data.result.content.pkgVersion")),
        }];
        this.telemetryHelper.logShareEvent(telemetryShareItems, "In", "Content");
    }
    private async checkDiskSpaceAvailability(zipSize, collection) {
        const availableDiskSpace = await this.systemSDK.getHardDiskInfo()
        .then(({availableHarddisk}) => availableHarddisk - 3e+8); // keeping buffer of 300 mb, this can be configured);
        if (!collection && (zipSize + (zipSize * 1.5) > availableDiskSpace)) {
            throw { message: "Disk space is low, couldn't copy Ecar" , code : "LOW_DISK_SPACE"};
        } else if (zipSize * 1.5 > availableDiskSpace) {
            throw { message: "Disk space is low, couldn't copy Ecar" , code : "LOW_DISK_SPACE"};
        }
    }
}
