import { Inject } from "typescript-ioc";
import DatabaseSDK from "../../sdk/database";
import * as _ from 'lodash';
import Response from '../../utils/response';
import { Manifest } from "@project-sunbird/ext-framework-server/models";
import { logger } from '@project-sunbird/ext-framework-server/logger';
import { containerAPI } from "OpenRAP/dist/api";
import { HTTPService } from "@project-sunbird/ext-framework-server/services";
import * as path from "path";
import { ImportStatus, IContentImport } from "../../manager/contentImportManager"
import { IAddedUsingType } from '../../controllers/content/IContent';
import TelemetryHelper from "../../helper/telemetryHelper";
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

let dbName = "content_download";
export default class ContentDownload {

    private contentsFilesPath: string = 'content';
    private ecarsFolderPath: string = 'ecars';

    @Inject
    private databaseSdk: DatabaseSDK;

    @Inject private telemetryHelper: TelemetryHelper;

    private downloadManager;
    private pluginId;
    private systemSDK;
    constructor(manifest: Manifest) {
        this.databaseSdk.initialize(manifest.id);
        this.pluginId = manifest.id;
        this.downloadManager = containerAPI.getDownloadManagerInstance(this.pluginId);
        this.systemSDK = containerAPI.getSystemSDKInstance(manifest.id);
    }

    download(req: any, res: any): any {
        (async () => {
            try {
                logger.debug(`ReqId = "${req.headers['X-msgid']}": Content Download method is called`);
                // get the content using content read api
                logger.debug(`ReqId = "${req.headers['X-msgid']}": Get the content using content read api`)
                let content = await HTTPService.get(`${process.env.APP_BASE_URL}/api/content/v1/read/${req.params.id}`, {}).toPromise()
                logger.info(`ReqId = "${req.headers['X-msgid']}": Content: ${_.get(content, 'data.result.content.identifier')} found from content read api`);
                if (_.get(content, 'data.result.content.mimeType')) {
                    // Adding telemetry share event
                    this.constructShareEvent(content);
                    // check if the content is type collection
                    logger.debug(`ReqId = "${req.headers['X-msgid']}": check if the content is of type collection`)
                    if (_.get(content, 'data.result.content.mimeType') !== "application/vnd.ekstep.content-collection") {
                        logger.info(`ReqId = "${req.headers['X-msgid']}": Found content:${_.get(content, 'data.result.content.mimeType')} is not of type collection`)
                        // insert to the to content_download_queue
                        // add the content to queue using downloadManager
                        const zipSize = (_.get(content, "data.result.content.size") as number);
                        await this.checkDiskSpaceAvailability(zipSize, false);
                        let downloadFiles = [{
                            id: (_.get(content, "data.result.content.identifier") as string),
                            url: (_.get(content, "data.result.content.downloadUrl") as string),
                            size: (_.get(content, "data.result.content.size") as number)
                        }]
                        let downloadId = await this.downloadManager.download(downloadFiles, 'ecars')
                        let queueMetaData = {
                            mimeType: _.get(content, 'data.result.content.mimeType'),
                            items: downloadFiles,
                            pkgVersion: _.get(content, 'data.result.content.pkgVersion'),
                            contentType: _.get(content, 'data.result.content.contentType'),
                            resourceId: _.get(content, "data.result.content.identifier")
                        }
                        logger.debug(`ReqId = "${req.headers['X-msgid']}": insert to the content_download_queue`);
                        await this.databaseSdk.insert(dbName, {
                            downloadId: downloadId,
                            contentId: _.get(content, "data.result.content.identifier"),
                            identifier: _.get(content, "data.result.content.identifier"),
                            name: _.get(content, "data.result.content.name"),
                            status: CONTENT_DOWNLOAD_STATUS.Submitted,
                            queueMetaData: queueMetaData,
                            createdOn: Date.now(),
                            updatedOn: Date.now(),
                            size: (_.get(content, "data.result.content.size") as number)
                        })
                        logger.info(`ReqId = "${req.headers['X-msgid']}": Content Inserted in Database Successfully`);
                        return res.send(Response.success("api.content.download", { downloadId }, req));
                        // return response the downloadId
                    } else {
                        logger.info(`ReqId = "${req.headers['X-msgid']}": Found content:${_.get(content, 'data.result.content.mimeType')} is of type collection`)
                        let downloadFiles = [{
                            id: (_.get(content, "data.result.content.identifier") as string),
                            url: (_.get(content, "data.result.content.downloadUrl") as string),
                            size: (_.get(content, "data.result.content.size") as number)
                        }];
                        let totalCollectionSize = _.get(content, "data.result.content.size");
                        // get the child contents
                        let childNodes = _.get(content, "data.result.content.childNodes")
                        if (!_.isEmpty(childNodes)) {
                            logger.debug(`ReqId = "${req.headers['X-msgid']}": Get the child contents using content search API`);
                            let childrenContentsRes = await HTTPService.post(`${process.env.APP_BASE_URL}/api/content/v1/search`,
                                {
                                    "request": {
                                        "filters": {
                                            "identifier": childNodes,
                                            "mimeType": { "!=": "application/vnd.ekstep.content-collection" }
                                        },
                                        "limit": childNodes.length
                                    }
                                }, {
                                headers: {
                                    "Content-Type": "application/json"
                                }
                            }).toPromise();
                            logger.info(`ReqId = "${req.headers['X-msgid']}": Found child contents: ${_.get(childrenContentsRes, 'data.result.count')}`);
                            if (_.get(childrenContentsRes, 'data.result.count')) {
                                let contents = _.get(childrenContentsRes, 'data.result.content');
                                for (let content of contents) {
                                    totalCollectionSize += _.get(content, "size");
                                    downloadFiles.push({
                                        id: (_.get(content, "identifier") as string),
                                        url: (_.get(content, "downloadUrl") as string),
                                        size: (_.get(content, "size") as number)
                                    })
                                }
                            }

                        }
                        let downloadId = await this.downloadManager.download(downloadFiles, 'ecars')
                        let queueMetaData = {
                            mimeType: _.get(content, 'data.result.content.mimeType'),
                            items: downloadFiles,
                            pkgVersion: _.get(content, 'data.result.content.pkgVersion'),
                            contentType: _.get(content, 'data.result.content.contentType'),
                            resourceId: _.get(content, "data.result.content.identifier")
                        }
                        await this.checkDiskSpaceAvailability(totalCollectionSize, true);
                        logger.debug(`ReqId = "${req.headers['X-msgid']}": insert collection in Database`);
                        await this.databaseSdk.insert(dbName, {
                            downloadId: downloadId,
                            contentId: _.get(content, "data.result.content.identifier"),
                            identifier: _.get(content, "data.result.content.identifier"),
                            name: _.get(content, "data.result.content.name"),
                            status: CONTENT_DOWNLOAD_STATUS.Submitted,
                            queueMetaData: queueMetaData,
                            createdOn: Date.now(),
                            updatedOn: Date.now(),
                            size: totalCollectionSize
                        })
                        logger.info(`ReqId = "${req.headers['X-msgid']}": Collection inserted successfully`);
                        return res.send(Response.success("api.content.download", { downloadId }, req));
                    }
                } else {
                    logger.error(`ReqId = "${req.headers['X-msgid']}": Received error while processing download request ${content}, for content ${req.params.id}`);
                    res.status(500)
                    return res.send(Response.error("api.content.download", 500))
                }

            } catch (error) {
                logger.error(`ReqId = "${req.headers['X-msgid']}": Received error while processing download request and err.message: ${error.message}, for content ${req.params.id}`);
                if (_.get(error, "code") === "LOW_DISK_SPACE") {
                    res.status(507);
                    return res.send(Response.error("api.content.download", 507, "Low disk space", "LOW_DISK_SPACE"));
                }
                res.status(500)
                return res.send(Response.error("api.content.download", 500))
            }
        })()
    }

    private constructShareEvent(content) {
        const telemetryShareItems = [{
            id: _.get(content, "data.result.content.identifier"),
            type: _.get(content, "data.result.content.contentType"),
            ver: _.toString(_.get(content, "data.result.content.pkgVersion")),
        }];
        this.telemetryHelper.logShareEvent(telemetryShareItems, "In", "Content");
    }

    list(req: any, res: any): any {
        (async () => {
            logger.debug(`ReqId = "${req.headers['X-msgid']}": ContentDownload List method is called`);
            try {
                let status = [API_DOWNLOAD_STATUS.submitted, API_DOWNLOAD_STATUS.inprogress,
                API_DOWNLOAD_STATUS.completed, API_DOWNLOAD_STATUS.failed,
                API_DOWNLOAD_STATUS.paused, API_DOWNLOAD_STATUS.canceled];
                if (!_.isEmpty(_.get(req, 'body.request.filters.status'))) {
                    status = _.get(req, 'body.request.filters.status');
                }
                let submitted = [];
                let inprogress = [];
                let failed = [];
                let completed = [];
                let paused = [];
                let canceled = [];
                let contentListArray = [];
                logger.debug(`ReqId = "${req.headers['X-msgid']}": Check download status is submitted or not`);
                if (_.indexOf(status, API_DOWNLOAD_STATUS.submitted) !== -1) {
                    // submitted - get from the content downloadDB and merge with data
                    logger.info(`ReqId = "${req.headers['X-msgid']}": download status is submitted`);
                    logger.debug(`ReqId = "${req.headers['X-msgid']}": Find submitted contents in ContentDb`)
                    const submittedDbData = await this.databaseSdk.find(dbName, {
                        "selector": {
                            "status": CONTENT_DOWNLOAD_STATUS.Submitted
                        }
                    });
                    logger.info(`ReqId = "${req.headers['X-msgid']}": Found Submitted Contents: ${submittedDbData.docs.length}`)
                    if (!_.isEmpty(submittedDbData.docs)) {
                        submitted = _.map(submittedDbData.docs, (doc) => {
                            return {
                                "id": doc.downloadId,
                                "contentId": doc.contentId,
                                "identifier": doc.contentId,
                                "resourceId": _.get(doc, 'queueMetaData.resourceId'),
                                "mimeType": doc.queueMetaData.mimeType,
                                "name": doc.name,
                                "status": ImportStatus[2],
                                "createdOn": doc.createdOn,
                                "pkgVersion": _.get(doc, 'queueMetaData.pkgVersion'),
                                "contentType": _.get(doc, 'queueMetaData.contentType'),
                                "totalSize": doc.size,
                                "addedUsing": "download"
                            };
                        })
                    }
                }
                logger.debug(`ReqId = "${req.headers['X-msgid']}": Check download status is completed or not`);
                if (_.indexOf(status, API_DOWNLOAD_STATUS.completed) !== -1) {
                    logger.info(`ReqId = "${req.headers['X-msgid']}": download status is completed`);
                    logger.debug(`ReqId = "${req.headers['X-msgid']}": Find completed contents in ContentDb`)
                    const completedDbData = await this.databaseSdk.find(dbName, {
                        "selector": {
                            "status": CONTENT_DOWNLOAD_STATUS.Indexed,
                            "updatedOn": {
                                "$gt": sessionStartTime
                            },
                            "createdOn": {
                                "$gt": null
                            }
                        },
                        "limit": 50,
                        "sort": [
                            {
                                "createdOn": "desc"
                            }
                        ]
                    });
                    if (!_.isEmpty(completedDbData.docs)) {
                        logger.info(`ReqId = "${req.headers['X-msgid']}": Found Submitted Contents: ${completedDbData.docs.length}`)
                        completed = _.map(completedDbData.docs, (doc) => {
                            return {
                                "id": doc.downloadId,
                                "contentId": doc.contentId,
                                "identifier": doc.contentId,
                                "resourceId": _.get(doc, 'queueMetaData.resourceId'),
                                "mimeType": doc.queueMetaData.mimeType,
                                "name": doc.name,
                                "status": ImportStatus[8],
                                "createdOn": doc.createdOn,
                                "pkgVersion": _.get(doc, 'queueMetaData.pkgVersion'),
                                "contentType": _.get(doc, 'queueMetaData.contentType'),
                                "totalSize": doc.size,
                                "addedUsing": "download"
                            };
                        })
                    }
                }

                // inprogress - get from download queue and merge with content data
                logger.debug(`ReqId = "${req.headers['X-msgid']}": Check download status is inprogress or not`);
                if (_.indexOf(status, API_DOWNLOAD_STATUS.inprogress) !== -1) {
                    logger.info(`ReqId = "${req.headers['X-msgid']}": download status is inprogress`);
                    const inprogressItems = await this.downloadManager.list(["INPROGRESS"]);
                    if (!_.isEmpty(inprogressItems)) {
                        let downloadIds = _.map(inprogressItems, 'id');
                        submitted = _.filter(submitted, (s) => { return _.indexOf(downloadIds, s.id) === -1 });
                        logger.debug(`ReqId = "${req.headers['X-msgid']}": Find inprogress contents in ContentDb`)
                        const inProgressDbData = await this.databaseSdk.find(dbName, {
                            "selector": {
                                "downloadId": {
                                    "$in": downloadIds
                                },
                                "createdOn": {
                                    "$gt": null
                                }
                            },
                            "sort": [
                                {
                                    "createdOn": "desc"
                                }
                            ]
                        });
                        logger.info(`ReqId = "${req.headers['X-msgid']}": Found inprogress Contents: ${inProgressDbData.docs.length}`)
                        _.forEach(inprogressItems, (item) => {
                            let contentItem = _.find(inProgressDbData.docs, { downloadId: item.id })
                            inprogress.push({
                                contentId: _.get(contentItem, 'contentId'),
                                identifier: _.get(contentItem, 'contentId'),
                                id: item.id,
                                resourceId: _.get(contentItem, 'queueMetaData.resourceId'),
                                name: _.get(contentItem, 'name') || 'Unnamed download',
                                totalSize: item.stats.totalSize,
                                downloadedSize: item.stats.downloadedSize,
                                status: ImportStatus[3],
                                createdOn: _.get(contentItem, 'createdOn') || item.createdOn,
                                addedUsing: "download"
                            })
                        })
                    }
                }


                // failed -  get from the content downloadDB and download queue
                logger.debug(`ReqId = "${req.headers['X-msgid']}": Check download status is failed or not`);
                if (_.indexOf(status, API_DOWNLOAD_STATUS.failed) !== -1) {
                    logger.info(`ReqId = "${req.headers['X-msgid']}": download status is failed`);
                    logger.debug(`ReqId = "${req.headers['X-msgid']}": Find Failed contents in ContentDb`)
                    const failedDbData = await this.databaseSdk.find(dbName, {
                        "selector": {
                            "status": CONTENT_DOWNLOAD_STATUS.Failed,
                            "updatedOn": {
                                "$gt": sessionStartTime
                            },
                            "createdOn": {
                                "$gt": null
                            }
                        },
                        "limit": 50,
                        "sort": [
                            {
                                "createdOn": "desc"
                            }
                        ]
                    });
                    if (!_.isEmpty(failedDbData.docs)) {
                        logger.info(`ReqId = "${req.headers['X-msgid']}": Found inprogress Contents: ${failedDbData.docs.length}`)
                        failed = _.map(failedDbData.docs, (doc) => {
                            return {
                                "id": doc.downloadId,
                                "contentId": doc.contentId,
                                "identifier": doc.contentId,
                                "resourceId": _.get(doc, 'queueMetaData.resourceId'),
                                "mimeType": doc.queueMetaData.mimeType,
                                "name": doc.name,
                                "status": ImportStatus[9],
                                "createdOn": doc.createdOn,
                                "pkgVersion": _.get(doc, 'queueMetaData.pkgVersion'),
                                "contentType": _.get(doc, 'queueMetaData.contentType'),
                                "totalSize": doc.size,
                                "addedUsing": "download"
                            };
                        })
                    }
                }

                // cancelled -  get from the content downloadDB
                logger.debug(`ReqId = "${req.headers['X-msgid']}": Check download status is canceled or not`);
                if (_.indexOf(status, API_DOWNLOAD_STATUS.canceled) !== -1) {
                    logger.info(`ReqId = "${req.headers['X-msgid']}": download status is canceled`);
                    logger.debug(`ReqId = "${req.headers['X-msgid']}": Find canceled contents in ContentDb`)
                    const canceledDbData = await this.databaseSdk.find(dbName, {
                        "selector": {
                            "status": CONTENT_DOWNLOAD_STATUS.Canceled,
                            "createdOn": {
                                "$gt": null
                            },
                        },
                        "limit": 50,
                        "sort": [
                            {
                                "createdOn": "desc"
                            }
                        ]
                    });
                    if (!_.isEmpty(canceledDbData.docs)) {
                        logger.info(`ReqId = "${req.headers['X-msgid']}": Found canceled Contents: ${canceledDbData.docs.length}`)
                        canceled = _.map(canceledDbData.docs, (doc) => {
                            return {
                                "id": doc.downloadId,
                                "contentId": doc.contentId,
                                "identifier": doc.contentId,
                                "resourceId": _.get(doc, 'queueMetaData.resourceId'),
                                "mimeType": doc.queueMetaData.mimeType,
                                "name": doc.name,
                                "status": ImportStatus[7],
                                "createdOn": doc.createdOn,
                                "pkgVersion": _.get(doc, 'queueMetaData.pkgVersion'),
                                "contentType": _.get(doc, 'queueMetaData.contentType'),
                                "totalSize": doc.size,
                                "addedUsing": "download"
                            };
                        })
                    }
                }

                // paused -  get from the content downloadDB and download queue
                logger.debug(`ReqId = "${req.headers['X-msgid']}": Check download status is paused or not`);
                if (_.indexOf(status, API_DOWNLOAD_STATUS.paused) !== -1) {
                    logger.info(`ReqId = "${req.headers['X-msgid']}": download status is paused`);
                    logger.debug(`ReqId = "${req.headers['X-msgid']}": Find paused contents in ContentDb`)
                    const pausedDbData = await this.databaseSdk.find(dbName, {
                        "selector": {
                            "status": CONTENT_DOWNLOAD_STATUS.Paused,
                            "createdOn": {
                                "$gt": null
                            },
                        },
                        "limit": 50,
                        "sort": [
                            {
                                "createdOn": "desc"
                            }
                        ]
                    });
                    if (!_.isEmpty(pausedDbData.docs)) {
                        logger.info(`ReqId = "${req.headers['X-msgid']}": Found paused Contents: ${pausedDbData.docs.length}`)
                        paused = _.map(pausedDbData.docs, (doc) => {
                            return {
                                "id": doc.downloadId,
                                "contentId": doc.contentId,
                                "identifier": doc.contentId,
                                "resourceId": _.get(doc, 'queueMetaData.resourceId'),
                                "mimeType": doc.queueMetaData.mimeType,
                                "name": doc.name,
                                "status": ImportStatus[5],
                                "createdOn": doc.createdOn,
                                "pkgVersion": _.get(doc, 'queueMetaData.pkgVersion'),
                                "contentType": _.get(doc, 'queueMetaData.contentType'),
                                "totalSize": doc.size,
                                "addedUsing": "download"
                            };
                        })
                    }
                }


                logger.info(`ReqId = "${req.headers['X-msgid']}": Received all downloaded Contents`);
                const importJobs = await this.listContentImport();
                contentListArray = _.concat(submitted, inprogress, failed, completed,
                    canceled, paused, importJobs.importList);

                return res.send(Response.success("api.content.download.list", {
                    response: {
                        contents: _.uniqBy(_.orderBy(contentListArray, ["createdOn"], ["desc"]), "contentId"),
                    }
                }, req));

            } catch (error) {
                logger.error(`ReqId = "${req.headers['X-msgid']}": Error while processing the list request and err.message: ${error.message}`)
                res.status(500)
                return res.send(Response.error("api.content.download.list", 500))
            }
        })()
    }
    // TODO:Query needs to be optimized
    public async listContentImport() {
        const importJobs = await this.databaseSdk.find('content_manager', {
            "selector": {
                $or: [
                    {
                        type: IAddedUsingType.import,
                        status: {
                            "$in": [ImportStatus.inProgress, ImportStatus.inQueue, ImportStatus.reconcile, ImportStatus.pausing, ImportStatus.paused,
                            ImportStatus.resume]
                        }
                    },
                    {
                        type: IAddedUsingType.import,
                        status: {
                            "$in": [ImportStatus.failed, ImportStatus.completed]
                        },
                        updatedOn: {
                            "$gt": sessionStartTime
                        }
                    }
                ]
            }
        }).catch((error) => {
            console.error('Error while fetching content import status');
            return { docs: [] }
        });
        const contentImportJobs = {
            importList: []
        };
        _.forEach(importJobs.docs, (job: IContentImport) => {
            const jobObj = {
                contentId: job.contentId,
                identifier: job.contentId,
                id: job._id,
                resourceId: job.contentId,
                name: job.name,
                totalSize: job.contentSize,
                downloadedSize: job.progress,
                status: ImportStatus[job.status],
                createdOn: job.createdOn,
                pkgVersion: job.pkgVersion,
                mimeType: job.mimeType,
                failedCode: job.failedCode,
                failedReason: job.failedReason,
                addedUsing: job.type
            }

            contentImportJobs.importList.push(jobObj)
        });
        return contentImportJobs;
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
    private async checkDiskSpaceAvailability(zipSize, collection) {
        const availableDiskSpace = await this.systemSDK.getHardDiskInfo()
        .then(({availableHarddisk}) => availableHarddisk - 3e+8); // keeping buffer of 300 mb, this can be configured);
        if (!collection || (zipSize + (zipSize * 1.5) > availableDiskSpace)) { 
            throw { message: "Disk space is low, couldn't copy Ecar" , code : "LOW_DISK_SPACE"};
        } else if (zipSize * 1.5 > availableDiskSpace) {
            throw { message: "Disk space is low, couldn't copy Ecar" , code : "LOW_DISK_SPACE"};
        }
    }
}
