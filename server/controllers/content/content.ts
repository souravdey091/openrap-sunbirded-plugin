import { Inject } from "typescript-ioc";
import DatabaseSDK from "../../sdk/database";
import * as _ from "lodash";
import config from "../../config";
import Response from "../../utils/response";
import { Manifest } from "@project-sunbird/ext-framework-server/models";
import * as Busboy from "busboy";
import * as fs from "fs";
import { logger } from "@project-sunbird/logger";
import * as path from "path";
import { ContentImportManager } from "../../manager/contentImportManager"
import * as uuid from "uuid";
import Hashids from "hashids";
import { containerAPI, ISystemQueueInstance } from "OpenRAP/dist/api";
import * as TreeModel from "tree-model";
import { HTTPService } from "@project-sunbird/ext-framework-server/services";
import { ExportContent } from "../../manager/contentExportManager"
import TelemetryHelper from "../../helper/telemetryHelper";
import { response } from "express";
const sessionStartTime = Date.now();

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
    private systemQueue: ISystemQueueInstance;

    private fileSDK;

    constructor(private manifest: Manifest) {
        this.contentImportManager.initialize();
        this.databaseSdk.initialize(manifest.id);
        this.fileSDK = containerAPI.getFileSDKInstance(manifest.id);
        this.systemQueue = containerAPI.getSystemQueueInstance(manifest.id);
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
                const downloadedContents = await this.changeContentStatus([content], req.headers['X-msgid']);
                if (downloadedContents.length > 0) {
                    content = downloadedContents[0];
                }
                if (this.isAvailableOffline(content)) {
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
                    contentDownloadList: _.map(_.get(data, 'metaData.contentDownloadList'),
                    (doc) => _.omit(doc, ["url"])),


                };
                listData.push(listObj);
            });
            return res.send(Response.success("api.content.list", {
                response: {
                    contents: _.uniqBy(_.orderBy(listData, ["createdOn"], ["desc"]), "contentId"),
                },
            }, req));
        } catch (error) {
            logger.error(`ReqId = "${req.headers['X-msgid']}": Error while processing the content list request and err.message: ${error.message}`);
            res.status(500);
            return res.send(Response.error("api.content.list", 500));
        }
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
            .then(async data => {
                data = _.map(data.docs, doc => _.omit(doc, ['_id', '_rev']));
                let resObj = {};
                let facets = [];
                if (_.get(reqBody, "request.facets")) {
                    facets = this.getFacets(_.get(reqBody, "request.facets"), data);
                }
                if (data.length === 0) {
                    logger.info(`ReqId = "${req.headers['X-msgid']}": Contents NOT found in DB`);
                    if (_.get(filters, "dialcodes")) {
                        const contents = await this.getMimeTypeCollections(_.get(filters, "dialcodes")[0]);
                        resObj = {
                            content: contents,
                            count: contents.length,
                            facets,
                        };
                    } else {
                        resObj = {
                            content: [],
                            count: 0,
                            facets,
                        };
                    }
                } else {
                    const downloadedContents = await this.changeContentStatus(data, req.headers['X-msgid']);
                    if (downloadedContents.length > 0) {
                        data = downloadedContents;
                    }
                    logger.info(`ReqId = "${req.headers['X-msgid']}": Contents = ${data.length} found in DB`)
                    resObj = {
                        content: data,
                        count: data.length,
                        facets,
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

    private async getMimeTypeCollections(dialCode: string) {
        const dbData = await this.databaseSdk.find("content", {
            selector: {
                mimeType: "application/vnd.ekstep.content-collection",
            },
        });

        if (dbData.docs.length) {
            for (const content of dbData.docs) {
                    const resp = await this.getDialCodeResources(content, dialCode);
                    if (!_.isEmpty(resp)) {
                        return resp;
                    }
            }
        }
        return [];
    }

    private async getDialCodeResources(content: {}, dialCode: string) {
        const model = new TreeModel();
        let treeModel;
        treeModel = model.parse(content);
        const contentIds: string[] = [];
        treeModel.walk((node) => {
            if (node.model.dialcodes && _.includes(node.model.dialcodes, dialCode)) {
                if (node.model.mimeType === "application/vnd.ekstep.content-collection") {
                    node.all((childNode) => {
                        if (childNode.model.mimeType !== "application/vnd.ekstep.content-collection") {
                            contentIds.push(childNode.model.identifier);
                        }
                    });
                } else {
                    contentIds.push(node.model.identifier);
                }
            }
        });
        if (_.isEmpty(contentIds)) {
            return [];
        }
        const dbFilter = {
            selector: {
                _id: {
                    $in: contentIds,
                },
            },
        };
        const dbData = await this.databaseSdk.find("content", dbFilter);
        return dbData.docs;
    }

    getFacets(facets, contents) {
        const facetData = [];
        if (contents.length === 0) {
            _.forEach(facets, (facet) => {
                facetData.push({name: facet, values: []});
            });
            return facetData;
        } else {
                _.forEach(facets, (facet) => {
                    let eachFacetData = _.map(contents, (content) => _.get(content, facet));
                    const arrayData = [];
                    _.forEach(eachFacetData, (data) => {
                        if (_.isArray(data)) {
                          _.map(data, (d) => arrayData.push(d));
                        } else {
                            arrayData.push(data);
                        }
                    });
                    eachFacetData = arrayData;
                    const result = _.values(_.groupBy(eachFacetData)).map((data) => {
                        if (!_.isEmpty(data[0])) {
                            return ({ name: data[0], count: data.length});
                        }
                    });
                    facetData.push({ name: facet, values: _.compact(result) || [] });
                });
                return facetData;
            }
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
            let offlineContents = await this.getAllOfflineContents(listOfContentIds, reqId);
            contents = await this.changeContentStatus(offlineContents.docs, reqId, contents);
            return contents;
        } catch (err) {
            logger.error(`ReqId = "${reqId}": Received  error err.message: ${err.message} ${err}`);
            return contents;
        }
    }

    /* This method is to check dialcode contents present in DB */

    decorateDialCodeContents(content, reqId) {
        logger.debug(`ReqId = "${reqId}": Decorating Dial Code Contents`);
        logger.debug(`ReqId = "${reqId}": Calling decorateContent from decoratedialcode`)
        return this.decorateContentWithProperty([content], reqId);
    }

    /* This method is to search contents for download status in database  */

    private isUpdateRequired(content, req) {
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

    private checkForUpdates(offlineContent, req) {
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

    // tslint:disable-next-line:member-ordering
    public async getOfflineContents(contentsIds: string[], reqId: string) {
        const dbFilter = {
            selector: {
                        identifier: {
                            $in: contentsIds,
                        },
                        $or: [
                            { "desktopAppMetadata.isAvailable": { $exists: false } },
                            { "desktopAppMetadata.isAvailable": { $eq: true } }
                        ]
            },
        };
        return await this.databaseSdk.find("content", dbFilter);
    }

    // tslint:disable-next-line:member-ordering
    public async getAllOfflineContents(contentsIds: string[], reqId: string) {
        const dbFilter = {
            selector: {
                        identifier: {
                            $in: contentsIds,
                        }
            },
        };
        return await this.databaseSdk.find("content", dbFilter);
    }

    // tslint:disable-next-line:member-ordering
    public isAvailableOffline(content) {
        return  (!_.has(content.desktopAppMetadata, "isAvailable") || content.desktopAppMetadata.isAvailable);
    }

    private async changeContentStatus(offlineContents, reqId, onlineContents?) {
        if (onlineContents) {
            offlineContents = _.map(onlineContents, (content) => {
                const data = _.find(offlineContents, { identifier: content.identifier });
                if (data && this.isAvailableOffline(data)) {
                    return data;
                } else {
                    if (data) {
                        content[`desktopAppMetadata`] = data["desktopAppMetadata"];
                    }
                    return content;
                }
            });
            return offlineContents;
        }
        return offlineContents;
    }

}