import { Inject } from "typescript-ioc";
import DatabaseSDK from "../../sdk/database";
import * as _ from "lodash";
import config from "../../config";
import Response from "../../utils/response";
import { Manifest } from "@project-sunbird/ext-framework-server/models";
import * as Busboy from "busboy";
import * as fs from "fs";
import { logger } from "@project-sunbird/ext-framework-server/logger";
import * as path from "path";
import { ContentImportManager } from "../../manager/contentImportManager"
import * as uuid from "uuid";
import Hashids from "hashids";
import { containerAPI } from "OpenRAP/dist/api";
import * as TreeModel from "tree-model";
import { HTTPService } from "@project-sunbird/ext-framework-server/services";
import { ExportContent } from "../../manager/contentExportManager"
import TelemetryHelper from "../../helper/telemetryHelper";
import { response } from "express";

export enum DOWNLOAD_STATUS {
    SUBMITTED = "DOWNLOADING",
    COMPLETED = "DOWNLOADING",
    EXTRACTED = "DOWNLOADING",
    INDEXED = "DOWNLOADED",
    FAILED = "FAILED",
    PAUSED = "PAUSED",
    CANCELED = "CANCELED",
}
const INTERVAL_TO_CHECKUPDATE = 1
export default class Content {
    private deviceId: string;
    private contentsFilesPath: string = 'content';
    private ecarsFolderPath: string = 'ecars';
    @Inject
    private databaseSdk: DatabaseSDK;

    @Inject private telemetryHelper: TelemetryHelper;

    @Inject
    private contentImportManager: ContentImportManager;

    private fileSDK;

    constructor(private manifest: Manifest) {
        this.contentImportManager.initialize();
        this.databaseSdk.initialize(manifest.id);
        this.fileSDK = containerAPI.getFileSDKInstance(manifest.id);
        this.getDeviceId();
    }

    searchInDB(filters, reqId, sort?) {
        logger.debug(`ReqId = "${reqId}": Contents are searching in ContentDb with given filters`)
        let modifiedFilters: Object = _.mapValues(filters, (v, k) => {
            if (k !== 'query') return ({ '$in': v })
        });
        delete modifiedFilters['query'];
        logger.info(`ReqId = "${reqId}": Deleted 'query' in modifiedFilters`);
        if (_.get(filters, 'query')) {
            modifiedFilters['name'] = {
                "$regex": new RegExp(_.get(filters, 'query'), 'i')
            }
        }
        modifiedFilters['visibility'] = "Default";
        modifiedFilters['$or'] = [
            {"desktopAppMetadata.isAvailable": { $exists: false}},
            {"desktopAppMetadata.isAvailable": { $eq: true}}
          ]
        let dbFilters = {
            selector: modifiedFilters,
            limit: parseInt(config.get('CONTENT_SEARCH_LIMIT'), 10)
        }
        if (sort) {
            logger.info(`ReqId = "${reqId}": Sort is present. Sorting the contents based on given sort properties`)
            for (let sortFields of Object.keys(sort)) {
                dbFilters.selector[sortFields] = {
                    "$gt": null
                }
            }
            dbFilters['sort'] = [sort];
        }
        logger.debug(`ReqId = "${reqId}": Find the contents in ContentDb with the given filters`)
        return this.databaseSdk.find('content', dbFilters);
    }

    get(req: any, res: any): any {
        (async () => {
            try {
                logger.debug(`ReqId = "${req.headers['X-msgid']}": Called Content get method to get Content: ${req.params.id} `);
                let id = req.params.id;
                logger.debug(`ReqId = "${req.headers['X-msgid']}": Get Content: ${id} from ContentDB`);
                let content = await this.databaseSdk.get('content', id);
                content = _.omit(content, ['_id', '_rev']);
                if (!_.has(content.desktopAppMetadata, "isAvailable") ||
                content.desktopAppMetadata.isAvailable) {
                let resObj = {};
                logger.debug(`ReqId = "${req.headers['X-msgid']}": Call isUpdateRequired()`)
                if (this.isUpdateRequired(content, req)) {
                    logger.debug(`ReqId = "${req.headers['X-msgid']}": Call checkForUpdate() to check whether update is required for content: `, _.get(content, 'identifier'));
                    content = await this.checkForUpdates(content, req)
                    resObj['content'] = content;
                    return res.send(Response.success('api.content.read', resObj, req));
                } else {
                    resObj['content'] = content;
                    return res.send(Response.success('api.content.read', resObj, req));
                }
            } else {
                res.status(404);
                return res.send(Response.error('api.content.read', 404));
            }
            } catch (error) {
                logger.error(
                    `ReqId = "${req.headers['X-msgid']}": Received error while getting the data from content database and err.message: ${error}`
                );
                if (error.status === 404) {
                    res.status(404);
                    return res.send(Response.error('api.content.read', 404));
                } else {
                    let status = error.status || 500;
                    res.status(status);
                    return res.send(Response.error('api.content.read', status));
                }
            }
        })()

    }

    search(req: any, res: any): any {
        logger.debug(`ReqId = "${req.headers['X-msgid']}": Called content search method`);
        let reqBody = req.body;
        let pageReqFilter = _.get(reqBody, 'request.filters');
        let contentSearchFields = config.get('CONTENT_SEARCH_FIELDS').split(',');
        logger.info(`ReqId = "${req.headers['X-msgid']}": picked filters from the request`);
        let filters = _.pick(pageReqFilter, contentSearchFields);
        filters = _.mapValues(filters, function (v) {
            return _.isString(v) ? [v] : v;
        });
        let query = _.get(reqBody, 'request.query');
        if (!_.isEmpty(query)) {
            filters.query = query;
        }
        logger.info(`ReqId = "${req.headers['X-msgid']}": Got query from the request`);
        logger.debug(`ReqId = "${req.headers['X-msgid']}": Searching Content in Db with given filters`)
        this.searchInDB(filters, req.headers['X-msgid'])
            .then(data => {
                data = _.map(data.docs, doc => _.omit(doc, ['_id', '_rev']));
                let resObj = {};
                if (data.length === 0) {
                    logger.info(`ReqId = "${req.headers['X-msgid']}": Contents NOT found in DB`);
                    resObj = {
                        content: [],
                        count: 0
                    };
                } else {
                    logger.info(`ReqId = "${req.headers['X-msgid']}": Contents = ${data.length} found in DB`)
                    resObj = {
                        content: data,
                        count: data.length
                    };
                }

                const responseObj = Response.success('api.content.search', resObj, req);
                this.constructSearchEdata(req, responseObj);
                return res.send(responseObj);
            })
            .catch(err => {
                console.log(err);
                logger.error(
                    `ReqId = "${req.headers['X-msgid']}":  Received error while searching content - err.message: ${
                    err.message
                    } ${err}`
                );
                if (err.status === 404) {
                    res.status(404);
                    return res.send(Response.error('api.content.search', 404));
                } else {
                    let status = err.status || 500;
                    res.status(status);
                    return res.send(Response.error('api.content.search', status));
                }
            });
    }

    public async import(req: any, res: any) {
        const ecarFilePaths = req.body;
        if (!ecarFilePaths) {
            return res.status(400).send(Response.error(`api.content.import`, 400, "MISSING_ECAR_PATH"));
        }
        this.contentImportManager.add(ecarFilePaths).then((jobIds) => {
            res.send(Response.success("api.content.import", {
                importedJobIds: jobIds,
            }, req));
        }).catch((err) => {
            res.status(500);
            res.send(Response.error(`api.content.import`, 400, err.errMessage || err.message, err.code));
        });
    }
    public async pauseImport(req: any, res: any) {
        this.contentImportManager.pauseImport(req.params.importId).then((jobIds) => {
            res.send(Response.success("api.content.import", {
                jobIds,
            }, req));
        }).catch((err) => {
            res.status(500);
            res.send(Response.error(`api.content.import`, 400, err.message));
        });
    }
    public async resumeImport(req: any, res: any) {
        this.contentImportManager.resumeImport(req.params.importId).then((jobIds) => {
            res.send(Response.success("api.content.import", {
                jobIds,
            }, req));
        }).catch((err) => {
            res.status(500);
            res.send(Response.error(`api.content.import`, 400, err.message));
        });
    }
    public async cancelImport(req: any, res: any) {
        await this.contentImportManager.cancelImport(req.params.importId).then((jobIds) => {
            res.send(Response.success("api.content.import", {
                jobIds,
            }, req));
        }).catch((err) => {
            res.status(500);
            res.send(Response.error(`api.content.import`, 400, err.message));
        });
    }
    public async retryImport(req: any, res: any) {
        this.contentImportManager.retryImport(req.params.importId).then((jobIds) => {
            res.send(Response.success("api.content.retry", {
                jobIds,
            }, req));
        }).catch((err) => {
            res.status(500);
            res.send(Response.error(`api.content.retry`, 400, err.message));
        });
    }
    public async export(req: any, res: any): Promise<any> {
        const id = req.params.id;
        const destFolder = req.query.destFolder;
        logger.debug(`ReqId = "${req.get("X-msgid")}": Get Content: ${id} from ContentDB`);
        const content = await this.databaseSdk.get("content", id);
        let childNode = [];
        if (content.mimeType === "application/vnd.ekstep.content-collection") {
            const dbChildResponse = await this.databaseSdk.find("content",
                {
                    selector: {
                        $and: [
                            {
                                _id: {
                                    $in: content.childNodes,
                                },
                            },
                            {
                                mimeType: {
                                    $nin: ["application/vnd.ekstep.content-collection"]
                                },
                            },
                        ],
                    },
                },
            );
            childNode = dbChildResponse.docs;
        }
        if (!_.has(content.desktopAppMetadata, "isAvailable") ||
        content.desktopAppMetadata.isAvailable) {
            const contentExport = new ExportContent(destFolder, content, childNode);
            contentExport.export((err, data) => {
                if (err) {
                    res.status(500);
                    return res.send(Response.error("api.content.export", 500));
                }
                // Adding telemetry share event
                const exportedChildContentCount = childNode.length - data.skippedContent.length;
                this.constructShareEvent(content, exportedChildContentCount);
                res.status(200);
                res.send(Response.success(`api.content.export`, {
                        response: {
                            ecarFilePath: data.ecarFilePath,
                        },
                    }, req));
            });
        } else {
            res.status(404);
            return res.send(Response.error("api.content.export", 404));
        }
    }

    public async getDeviceId() {
        this.deviceId = await containerAPI.getSystemSDKInstance(this.manifest.id).getDeviceId();
    }

    private constructSearchEdata(req, res) {
        const edata = {
            type: "content",
            query: _.get(req, "body.request.query"),
            filters: _.get(req, "body.request.filters"),
            correlationid: _.get(res, "params.msgid"),
            size: _.get(res, "result.count"),
        };
        this.telemetryHelper.logSearchEvent(edata, "Content");
    }

    private async constructShareEvent(data, childCount) {
        const transfers = 1 + childCount;
        const telemetryShareItems = [{
            id: _.get(data, "contentId"),
            type: _.get(data, "contentType"),
            ver: _.toString(_.get(data, "pkgVersion")),
            params: [
                { transfers: _.toString(transfers) },
                { size: _.toString(_.get(data, "size")) },
            ],
            origin: {
                id: this.deviceId,
                type: "Device",
            },
        }];
        this.telemetryHelper.logShareEvent(telemetryShareItems, "Out", "Content");
    }

    /* This method converts the buffer data to json and if any error will catch and return the buffer data */

    convertBufferToJson(proxyResData, req) {
        logger.debug(`ReqId = "${req.headers['X-msgid']}": Converting Bufferdata to json`)
        let proxyData;
        try {
            proxyData = JSON.parse(proxyResData.toString('utf8'));
        } catch (e) {
            console.log(e);
            logger.error(
                `ReqId = "${req.headers['X-msgid']}": Received error while parsing the Bufferdata to json: ${e}`
            );
            return proxyResData;
        }
        logger.info(`ReqId = "${req.headers['X-msgid']}": Succesfully converted Bufferdata to json`)
        return proxyData;
    }

    /*This method is to whether content is present and to store all the contents in all page sections to one array */

    decorateSections(sections, reqId) {
        logger.debug(`ReqId = "${reqId}": Called decorateSections to decorate content`)
        let contents = [];
        logger.info(`ReqId = "${reqId}": Fetching all the contentId's from all the sections into an array`);
        for (let section of sections) {
            if (!_.isEmpty(section.contents)) {
                for (let content of section.contents) {
                    contents.push(content);
                }
            }
        }
        logger.debug(`ReqId = "${reqId}": Calling decorateContent from decoratesections`)
        return this.decorateContentWithProperty(contents, reqId);
    }

    /* This method is to check contents are present in DB */

    async decorateContentWithProperty(contents, reqId) {
        logger.debug(`ReqId = "${reqId}": Called decorateContent to decorate content`)
        try {
            const listOfContentIds = [];
            logger.info(`ReqId = "${reqId}": Pushing all the contentId's to an Array for all the requested Contents`)
            for (const content of contents) {
                listOfContentIds.push(content.identifier);
            }
            logger.debug(`ReqId = "${reqId}": Search downloaded and downloading  contents in DB using content Id's`)
            const contentsInDownload = await this.searchDownloadingContent(listOfContentIds, reqId);
            const contentsInDB = await this.getOfflineContents(listOfContentIds, reqId);
            contents =  this.changeContentStatus(contentsInDownload.docs, contentsInDB.docs, contents);
            return contents;
        } catch (err) {
            logger.error(`ReqId = "${reqId}": Received  error err.message: ${err.message} ${err}`);
            return contents;
        }
    }

    /* This method is to check dialcode contents present in DB */

    decorateDialCodeContents(content, reqId) {
        logger.debug(`ReqId = "${reqId}": Decorating Dial Code Contents`);
        const model = new TreeModel();
        let treeModel;
        treeModel = model.parse(content);
        let contents = [];
        contents.push(content);
        logger.info(`ReqId = "${reqId}": walking through all the nodes and pushing all the child nodes to an array`);
        treeModel.walk(node => {
            if (node.model.mimeType !== 'application/vnd.ekstep.content-collection') {
                contents.push(node.model);
            }
        });
        logger.debug(`ReqId = "${reqId}": Calling decorateContent from decoratedialcode`)
        return this.decorateContentWithProperty(contents, reqId);
    }

    /* This method is to search contents for download status in database  */

    searchDownloadingContent(contents, reqId) {
        logger.debug(`ReqId = "${reqId}": searchDownloadingContent method is called`);
        let dbFilters = {
            "selector": {
                "contentId": {
                    "$in": contents
                }
            }
        }
        logger.info(`ReqId = "${reqId}": finding downloading, downloaded or failed contents in database`)
        return this.databaseSdk.find('content_download', dbFilters)
    }

    isUpdateRequired(content, req) {
        logger.debug(`ReqId = "${req.headers['X-msgid']}": Called isUpdateRequired()`);

        if (_.get(content, 'desktopAppMetadata.updateAvailable')) {
            logger.info(`ReqId = "${req.headers['X-msgid']}": updateAvailble for content and Don't call API`);
            return false;
        } else if (_.get(content, 'desktopAppMetadata.lastUpdateCheckedOn')) {
            logger.info(`ReqId = "${req.headers['X-msgid']}": checking when is the last updatechecked on`, _.get(content, 'desktopAppMetadata.lastUpdateCheckedOn'));
            return ((Date.now() - _.get(content, 'desktopAppMetadata.lastUpdateCheckedOn')) / 3600000) > INTERVAL_TO_CHECKUPDATE ? true : false;
        }
        logger.info(`ReqId = "${req.headers['X-msgid']}": update is not available for content and call API`);
        return true;
    }


    checkForUpdates(offlineContent, req) {
        logger.debug(`ReqId = "${req.headers['X-msgid']}": calling api to check whether content: ${_.get(offlineContent, 'idenitifier')} is updated`);
        return new Promise(async (resolve, reject) => {
            try {
                let onlineContent = await HTTPService.get(`${process.env.APP_BASE_URL}/api/content/v1/read/${offlineContent.identifier}?field=pkgVersion`, {}).toPromise();
                if (_.get(offlineContent, 'pkgVersion') < _.get(onlineContent, 'data.result.content.pkgVersion')) {
                    offlineContent.desktopAppMetadata.updateAvailable = true;
                }
                offlineContent.desktopAppMetadata.lastUpdateCheckedOn = Date.now();
                await this.databaseSdk.update('content', offlineContent.identifier, offlineContent);
                resolve(offlineContent);
            } catch (err) {
                logger.error(`ReqId = "${req.headers['X-msgid']}": Error occured while checking content update : ${err}`);
                resolve(offlineContent);
            }
        })
    }

    async getOfflineContents(contentsIds: string[], reqId: string) {
        const dbFilter = {
            selector: {
                identifier: {
                    $in: contentsIds,
                },
            },
            fields: ["desktopAppMetadata", "downloadStatus", "identifier"],
        };
        return await this.databaseSdk.find("content", dbFilter);
    }

    changeContentStatus(contentsInDownload, offlineContents, contents) {
        for (const content of offlineContents) {
            if (!_.has(content, "desktopAppMetadata.isAvailable") || content.desktopAppMetadata.isAvailable) {
                const data = _.find(contentsInDownload, { identifier: content.identifier })
                content.downloadStatus = !_.isEmpty(_.get(data, 'status')) ? DOWNLOAD_STATUS[data["status"]] : '';
            }
        }
        for (const content of contents) {
            const data = _.find(offlineContents, { identifier: content.identifier });
            content["downloadStatus"] = _.get(data, 'downloadStatus');
            content["desktopAppMetadata"] = _.get(data, 'desktopAppMetadata');
        }
        return contents;
    }

}
