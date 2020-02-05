import { logger } from "@project-sunbird/ext-framework-server/logger";
import * as _ from "lodash";
import { Inject } from "typescript-ioc";
import { manifest } from "../../manifest";
import DatabaseSDK from "../../sdk/database";
import { containerAPI, ISystemQueueInstance, ISystemQueue, SystemQueueStatus } from "OpenRAP/dist/api";

export default class SystemQueueMigration {
    @Inject private dbSDK: DatabaseSDK;
    private systemQueue: ISystemQueueInstance;

    public initialize() {
        this.dbSDK.initialize(manifest.id);
        this.systemQueue = containerAPI.getSystemQueueInstance(manifest.id);
        this.download();
        this.import();
    }

    private async download() {
        try {
            const dbData = await this.dbSDK.find("content_download",
                {
                    selector: {
                        status: {
                            $in: ['SUBMITTED', 'PAUSED', 'EXTRACTED']
                        },
                    },
                });
            if (!dbData && !dbData.docs) {
                logger.info('No content download data found for migrating to system_queue DB');
                return;
            }
            const reqData: ISystemQueue[] = [];
            _.forEach(dbData.docs, (data) => {
                const reqObj = {
                    _id: _.get(data, '_id'),
                    type: 'DOWNLOAD',
                    name: _.get(data, 'name'),
                    group: 'CONTENT_MANAGER',
                    createdOn: _.get(data, 'createdOn'),
                    updatedOn: _.get(data, 'updatedOn'),
                    isActive: true,
                    priority: 1,
                    progress: 0,
                    plugin: "openrap-sunbirded-plugin",
                    runTime: 0,
                    status: _.get(data, 'status') === 'PAUSED' ? SystemQueueStatus.paused : SystemQueueStatus.inQueue,
                    metaData: {
                        contentSize: _.get(data, 'size'),
                        contentId: _.get(data, 'queueMetaData.resourceId'),
                        mimeType: _.get(data, 'queueMetaData.mimeType'),
                        contentType: _.get(data, 'queueMetaData.contentType'),
                        pkgVersion: _.get(data, 'queueMetaData.pkgVersion'),
                        items: _.get(data, 'queueMetaData.items'),
                    },
                };
                reqData.push(reqObj);
            });
            this.systemQueue.migrate(reqData);
            logger.info('Content download data migrated successfully to system_queue DB');
        } catch (error) {
            logger.error(`Got error while migrating content download db data to system queue db ${error}`);
        }
    }

    private async import() {
        try {
            const dbData = await this.dbSDK.find("content_download",
                {
                    selector: {
                        status: {
                            $in: [0, 1, 2, 3, 4, 5],
                        },
                    },
                });
            if (!dbData && !dbData.docs) {
                logger.info('No content download data found for migrating to system_queue DB');
                return;
            }
            const reqData: ISystemQueue[] = [];
            _.forEach(dbData.docs, (data) => {
                const reqObj = {
                    _id: _.get(data, '_id'),
                    type: 'IMPORT',
                    name: _.get(data, 'name'),
                    group: 'CONTENT_MANAGER',
                    createdOn: _.get(data, 'createdOn'),
                    updatedOn: _.get(data, 'updatedOn'),
                    isActive: true,
                    priority: 1,
                    progress: _.get(data, 'progress'),
                    plugin: "openrap-sunbirded-plugin",
                    runTime: 0,
                    status: _.get(data, 'status') === 5 ? SystemQueueStatus.paused : SystemQueueStatus.inQueue,
                    metaData: {
                        contentSize: _.get(data, 'contentSize'),
                        contentId: _.get(data, 'contentId'),
                        mimeType: _.get(data, 'mimeType'),
                        contentType: _.get(data, 'contentType'),
                        pkgVersion: _.get(data, 'pkgVersion'),
                        extractedEcarEntries: _.get(data, 'pkgVersion'),
                        artifactUnzipped: _.get(data, 'pkgVersion'),
                        childNodes: _.get(data, 'pkgVersion'),
                        contentSkipped: _.get(data, 'pkgVersion'),
                        contentAdded: _.get(data, 'pkgVersion'),
                        ecarSourcePath: _.get(data, 'ecarSourcePath'),
                        step: _.get(data, 'importStep')
                    },
                };
                reqData.push(reqObj);
            });
            this.systemQueue.migrate(reqData);
            logger.info('Content download data migrated successfully to system_queue DB');
        } catch (error) {
            logger.error(`Got error while migrating content download db data to system queue db ${error}`);
        }
    }
}
