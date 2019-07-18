import { Inject } from 'typescript-ioc';
import * as path from 'path';
import * as glob from 'glob';
import * as _ from 'lodash';
import DatabaseSDK from './../sdk/database';
import { logger } from '@project-sunbird/ext-framework-server/logger';
import * as fs from 'fs';
import * as uuid from 'uuid';
import * as fse from 'fs-extra';
import { containerAPI } from 'OpenRAP/dist/api';
import { manifest } from '../manifest';
import { isRegExp } from 'util';
import config from '../config';
import { IDesktopAppMetadata, IAddedUsingType } from '../controllers/content/IContent';


export default class ContentManager {

    private pluginId: string;
    private contentFilesPath: string;
    private downloadsFolderPath: string;


    private fileSDK;

    @Inject dbSDK: DatabaseSDK;

    private watcher: any;

    initialize(pluginId, contentFilesPath, downloadsFolderPath) {
        this.pluginId = pluginId;
        this.downloadsFolderPath = downloadsFolderPath;
        this.contentFilesPath = contentFilesPath;
        this.dbSDK.initialize(pluginId);
        this.fileSDK = containerAPI.getFileSDKInstance(manifest.id);
    }



    // unzip ecar 
    // read manifest
    // check if the ecar is content or collection
    // if content
    // unzip internal folder and update/insert content db
    // if collection
    // if it has only one manifest.json
    // prepare hierarchy and insert/update in content db
    // if it has manifest with content folders   
    // prepare hierarchy and insert   
    async startImport(req) {
        logger.debug(`ReqID = "${req.headers['X-msgid']}": File extraction is started for the file: ${req.fileName}`)
        // unzip to content_files folder
        logger.info(` ReqID = "${req.headers['X-msgid']}": File has to be unzipped`);
        await this.fileSDK.unzip(path.join('ecars', req.fileName), 'content', true)
        logger.info(` ReqID = "${req.headers['X-msgid']}": File is unzipped, reading manifest file and adding baseDir to manifest`);
        // read manifest file and add baseDir to manifest as content and folder name relative path
        let manifest = await this.fileSDK.readJSON(path.join(this.contentFilesPath, path.basename(req.fileName, path.extname(req.fileName)), 'manifest.json'));
        let items = _.get(manifest, 'archive.items');
        if (items && _.isArray(items) &&
            items.length > 0) {
            // check if it is collection type or not   
            logger.debug(`ReqID = "${req.headers['X-msgid']}": checking if the content is of type collection or not`);
            let parent: any | undefined = _.find(items, (i) => {
                return (i.mimeType === 'application/vnd.ekstep.content-collection' && i.visibility === 'Default')
            });

            if (parent) {
                logger.info(` ReqID = "${req.headers['X-msgid']}": Found content is of type collection`);
                // check content compatibility level 
                logger.debug(` ReqID = "${req.headers['X-msgid']}": Checking content compatability. Collection compatabilitylevel > content compatabilitylevel`);
                if (_.get(parent, 'compatibilityLevel') && parent.compatibilityLevel > config.get("CONTENT_COMPATIBILITY_LEVEL")) {
                    throw `content compatibility is higher then content level : ${parent.compatibilityLevel} app supports ${config.get("CONTENT_COMPATIBILITY_LEVEL")}`;
                }
                logger.info(` ReqID = "${req.headers['X-msgid']}": collection compatability > content compatability level`);
                let itemsClone = _.cloneDeep(items);
                logger.debug(`ReqID = "${req.headers['X-msgid']}": Has to create Hierarchy for the Parent collection: ${_.get(parent, 'identifier')}  versionNumber: ${_.get(parent, 'pkgVersion')} and versionKey: ${_.get(parent, 'versionKey')}`);
                let children = this.createHierarchy(itemsClone, parent, req.headers['X-msgid'])
                logger.info(` ReqID = "${req.headers['X-msgid']}": Hierarchy is created for the collection ${_.get(parent, 'identifier')}`)
                parent['children'] = children;
                parent.desktopAppMetadata = {
                    "ecarFile": req.fileName,  // relative to ecar folder
                    "addedUsing": "import",
                    "createdOn": Date.now(),
                    "updatedOn": Date.now()
                }
                logger.info(` ReqID = "${req.headers['X-msgid']}":  Collection: ${_.get(parent, 'identifier')} has to be upserted in database`);
                await this.dbSDK.upsert('content', parent.identifier, parent);
                logger.info(` ReqID = "${req.headers['X-msgid']}": Collection is upserted in ContentDB `)
                let resources = _.filter(items, (i) => {
                    return (i.mimeType !== 'application/vnd.ekstep.content-collection')
                });
                logger.info(` ReqID = "${req.headers['X-msgid']}": Inserting the resources in collection to ContentDB`)
                //insert the resources to content db
                if (!_.isEmpty(resources)) {
                    await resources.forEach(async (resource) => {
                        logger.info(` ReqID = "${req.headers['X-msgid']}": including baseDir for all the resources in collection`)
                        // if (_.indexOf(['application/vnd.ekstep.ecml-archive', 'application/vnd.ekstep.html-archive'], resource.mimeType) >= 0) {
                        resource.baseDir = `content/${resource.identifier}`;
                        // } else {
                        //     resource.baseDir = 'content';
                        // }

                        resource.appIcon = resource.appIcon ? `content/${resource.appIcon}` : resource.appIcon;
                        logger.debug(`ReqID = "${req.headers['X-msgid']}": added baseDir for Resources and inserting in ContentDB`)
                        await this.dbSDK.upsert('content', resource.identifier, resource);
                        logger.info(` ReqID = "${req.headers['X-msgid']}": Resources are inserted in ContentDB`)
                    })
                }

                //copy directores to content files folder with manifest
                logger.info(` ReqID = "${req.headers['X-msgid']}": coping directories to content files folder with manifest`)
                let parentDirPath = path.join(this.contentFilesPath, path.basename(req.fileName, path.extname(req.fileName)));
                fs.readdir(parentDirPath, async (err, files) => {
                    //handling error
                    if (err) {
                        logger.error(`ReqID = "${req.headers['X-msgid']}": Error while reading the directory when importing collection`, err)
                    } else {
                        files.forEach(async (file) => {
                            fs.lstat(path.join(parentDirPath, file), async (err, stats) => {
                                if (err) {
                                    logger.error(`ReqID = "${req.headers['X-msgid']}": Error while reading files from collection directory`, err)
                                } else {
                                    if (stats.isDirectory()) {
                                        let manifest = {
                                            "id": "content.archive",
                                            "ver": "1.0",
                                            "ts": new Date().toISOString(),
                                            "params": {
                                                "resmsgid": uuid.v4()
                                            },
                                            "archive": {
                                                "count": 1,
                                                "items": []
                                            }
                                        }

                                        let item = _.find(items, { identifier: file })
                                        if (!_.isEmpty(item)) {
                                            manifest.archive.items.push(item)
                                            logger.info(` ReqID = "${req.headers['X-msgid']}": created manifest for the file ${file}`);
                                        }
                                        await fse.ensureFile(path.join(parentDirPath, file, 'manifest.json')).catch(err => {
                                            if (err) {
                                                logger.error(`ReqID = "${req.headers['X-msgid']}": Error while creating manifest for file ${file}`, err);
                                            }
                                        })
                                        await fse.outputJson(path.join(parentDirPath, file, 'manifest.json'), manifest).catch(err => {
                                            if (err) {
                                                logger.error(`ReqID = "${req.headers['X-msgid']}": Error while updating manifest for file ${file} with manifest ${manifest}`, err);
                                            }
                                        })
                                        await fse.copy(path.join(parentDirPath, file), path.join(this.contentFilesPath, file)).catch(err => {
                                            if (err) {
                                                logger.error(`ReqID = "${req.headers['X-msgid']}": Error while copying the folder ${path.join(parentDirPath, file)} to content files from collection`, err);
                                            }
                                        })
                                        let zipFilePath = glob.sync(path.join(this.contentFilesPath, file, '**', '*.zip'), {});
                                        if (zipFilePath.length > 0) {
                                            // unzip the file if we have zip file
                                            logger.info(` ReqID = "${req.headers['X-msgid']}":  Unzipping the file:${file} if the file is zip file`)
                                            let filePath = path.relative(this.fileSDK.getAbsPath(''), zipFilePath[0]);
                                            await this.fileSDK.unzip(filePath, path.join("content", file), false)
                                            logger.info(` ReqID = "${req.headers['X-msgid']}":   file is unzipped`)
                                        }
                                    }
                                }
                            })
                        });
                    }
                })
                return parent;
            } else {

                logger.info(` ReqID = "${req.headers['X-msgid']}": Found Content is not of type Collection`);
                // check content compatibility level 
                let metaData = items[0];
                logger.info(` ReqID = "${req.headers['X-msgid']}": check (resource) content compatability > content compatability level`);
                if (_.get(metaData, 'compatibilityLevel') && metaData.compatibilityLevel > config.get("CONTENT_COMPATIBILITY_LEVEL")) {
                    throw `content compatibility is higher then content level : ${metaData.compatibilityLevel} app supports ${config.get("CONTENT_COMPATIBILITY_LEVEL")}`;
                }
                logger.info(` ReqID = "${req.headers['X-msgid']}": (resource) content compatability > content compatability level`);
                //try to get zip file inside the unzip folder from above step
                let assetFolderGlobPath = path.join(this.contentFilesPath, path.basename(req.fileName, path.extname(req.fileName)), '**', '*.zip')

                let zipFilePath = glob.sync(assetFolderGlobPath, {});
                if (zipFilePath.length > 0) {
                    let filePath = path.relative(this.fileSDK.getAbsPath(''), zipFilePath[0]);
                    // unzip the file if we have zip file
                    logger.info(` ReqID = "${req.headers['X-msgid']}": Unzipping the file if there are any zip files`)
                    await this.fileSDK.unzip(filePath, path.join("content", path.basename(req.fileName, path.extname(req.fileName))), false)
                    logger.info(` ReqID = "${req.headers['X-msgid']}": Unzipped the zip file `)
                }

                metaData.baseDir = `content/${path.basename(req.fileName, path.extname(req.fileName))}`;
                metaData.appIcon = metaData.appIcon ? `content/${path.basename(req.fileName, path.extname(req.fileName))}/${metaData.appIcon}` : metaData.appIcon;
                const desktopAppMetadata: IDesktopAppMetadata = {
                    "ecarFile": req.fileName,  // relative to ecar folder
                    "addedUsing": IAddedUsingType.import,
                    "createdOn": Date.now(),
                    "updatedOn": Date.now()
                }
                logger.info(` ReqID = "${req.headers['X-msgid']}": Metadata and basedir is added for the (resource) content`);
                metaData.desktopAppMetadata = desktopAppMetadata;
                //insert metadata to content database
                // TODO: before insertion check if the first object is type of collection then prepare the collection and insert
                logger.debug(`ReqID = "${req.headers['X-msgid']}": (Resource) Content is upserting in ContentDB`)
                await this.dbSDK.upsert('content', metaData.identifier, metaData);
                return metaData;
            }

        } else {
            logger.error(`ReqID = "${req.headers['X-msgid']}": Ecar is having empty items `, manifest);
            throw Error(`ReqID = "${req.headers['X-msgid']}": Manifest doesn't have items to insert in database`)
        }
    }

    createHierarchy(items: any[], parent: any, reqID?: any,tree?: any[]): any {
        logger.debug(`ReqID = "${reqID}": creating Hierarchy for the Collection`);
        logger.info(` ReqID = "${reqID}": Getting child contents for Parent: ${_.get(parent, 'identifier')}`);
        tree = typeof tree !== 'undefined' ? tree : [];
        parent = typeof parent !== 'undefined' ? parent : { visibility: 'Default' };
        if (parent.children && parent.children.length) {
            let children = [];
            _.forEach(items, (child) => {
                let childWithIndex = _.find(parent.children, { 'identifier': child.identifier })
                if (!_.isEmpty(childWithIndex)) {
                    child.index = childWithIndex['index'];
                    children.push(child)
                }
            });
            if (!_.isEmpty(children)) {
                children = _.sortBy(children, 'index');
                if (parent.visibility === 'Default') {
                    tree = children;
                } else {
                    parent['children'] = children;
                }
                _.each(children, (child) => { this.createHierarchy(items, child, reqID) });
            }
        }
        logger.info(` ReqID = "${reqID}": Child contents are found for Parent: ${_.get(parent, 'identifier')}`);
        return tree;
    }

}