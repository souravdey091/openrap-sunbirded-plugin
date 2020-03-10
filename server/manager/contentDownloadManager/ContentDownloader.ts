import * as childProcess from "child_process";
import { Inject } from "typescript-ioc";
import * as path from "path";
import DatabaseSDK from "../../sdk/database";
import { logger } from "@project-sunbird/ext-framework-server/logger";
import { manifest } from "../../manifest";
import * as  _ from "lodash";
import { Observer } from "rxjs";
import { containerAPI, ITaskExecuter, ISystemQueue, SystemQueueStatus } from "OpenRAP/dist/api";
import { IDownloadMetadata, IContentDownloadList } from "./IContentDownload";
import * as  StreamZip from "node-stream-zip";
import TelemetryHelper from "../../helper/telemetryHelper";
import * as os from "os";

export class ContentDownloader implements ITaskExecuter {
  public static taskType = "DOWNLOAD";
  public static group = "CONTENT_MANAGER";
  private contentDownloadData: ISystemQueue;
  @Inject private databaseSdk: DatabaseSDK;
  @Inject private telemetryHelper: TelemetryHelper;
  private downloadSDK = containerAPI.getDownloadSdkInstance();
  private observer: Observer<ISystemQueue>;
  private fileSDK = containerAPI.getFileSDKInstance(manifest.id);
  private systemSDK = containerAPI.getSystemSDKInstance(manifest.id);
  private contentDownloadMetaData: IDownloadMetadata;
  private ecarBasePath = this.fileSDK.getAbsPath("ecars");
  private interrupt = false;
  private interruptType: "PAUSE" | "CANCEL";
  private downloadFailedCount = 0;
  private extractionFailedCount = 0;
  private downloadContentCount = 0;
  public async start(contentDownloadData: ISystemQueue, observer: Observer<ISystemQueue>) {
    this.databaseSdk.initialize(manifest.id);
    this.contentDownloadData = contentDownloadData;
    this.observer = observer;
    this.contentDownloadMetaData = this.contentDownloadData.metaData;
    logger.debug("ContentDownload executer start method called", this.contentDownloadData._id);
    _.forIn(this.contentDownloadMetaData.contentDownloadList, (value: IContentDownloadList, key) => {
      this.downloadContentCount += 1;
      if (value.step === "DOWNLOAD") {
        this.downloadSDK.queueDownload(value.downloadId, {
          url: value.url,
          savePath: path.join(this.ecarBasePath, value.downloadId),
        }, this.getDownloadObserver(value.identifier));
      } else {
        this.handleDownloadComplete(value.identifier, value);
      }
    });
    return true;
  }

  public status(): ISystemQueue {
    return this.contentDownloadData;
  }
  public async pause() {
    this.interrupt = false;
    this.interruptType = "PAUSE";
    let pausedInQueue = false;
    logger.debug("ContentDownload pause method called", this.contentDownloadData._id);
    _.forIn(this.contentDownloadMetaData.contentDownloadList, (value: IContentDownloadList, key) => {
      if (value.step === "DOWNLOAD") {
        const pauseRes = this.downloadSDK.cancel(value.downloadId);
        if (pauseRes) {
          pausedInQueue = true;
        }
      }
    });
    if (pausedInQueue) {
      return true;
    } else {
      return {
          code: "NO_FILES_IN_QUEUE",
          status: 400,
          message: `No files are in queue`,
        };
    }
  }
  public async resume(contentDownloadData: ISystemQueue, observer: Observer<ISystemQueue>) {
    logger.debug("ContentDownload executer resume method called", this.contentDownloadData._id, "calling start method");
    return this.start(contentDownloadData, observer);
  }
  public async cancel() {
    this.interrupt = false;
    this.interruptType = "CANCEL";
    let cancelInQueue = false;
    logger.debug("ContentDownload pause method called", this.contentDownloadData._id);
    _.forIn(this.contentDownloadMetaData.contentDownloadList, (value: IContentDownloadList, key) => {
      if (value.step === "DOWNLOAD") {
        const cancelRes = this.downloadSDK.cancel(value.downloadId);
        if (cancelRes) {
          cancelInQueue = true;
        }
      }
    });
    if (cancelInQueue) {
      return true;
    } else {
      return {
          code: "NO_FILES_IN_QUEUE",
          status: 400,
          message: `No files are in queue`,
        };
    }
  }
  public async retry(contentDownloadData: ISystemQueue, observer: Observer<ISystemQueue>) {
    logger.debug("ContentDownload executer retry method called", this.contentDownloadData._id, "calling retry method");
    return this.start(contentDownloadData, observer);
  }
  private getDownloadObserver(contentId) {
    const next = (downloadProgress: IDownloadProgress) => {
      logger.debug(`${this.contentDownloadData._id}:Download progress event contentId: ${contentId}`, downloadProgress);
      if (downloadProgress.total) {
        this.handleDownloadProgress(contentId, downloadProgress);
      }
    };
    const error = (downloadError) => {
      this.handleDownloadError(contentId, downloadError);
    };
    const complete = () => {
      logger.debug(`${this.contentDownloadData._id}:Download complete event contentId: ${contentId}`);
      const contentDetails = this.contentDownloadMetaData.contentDownloadList[contentId];
      contentDetails.step = "EXTRACT";
      this.contentDownloadMetaData.downloadedSize += contentDetails.size;
      this.observer.next(this.contentDownloadData);
      this.handleDownloadComplete(contentId, contentDetails);
    };
    return { next, error, complete };
  }
  private handleDownloadError(contentId, error) {
    logger.debug(`${this.contentDownloadData._id}:Download error event contentId: ${contentId},`, error);
    this.downloadFailedCount += 1;
    if (_.includes(["ESOCKETTIMEDOUT"], _.get(error, 'code')) || this.downloadFailedCount > 1 || (this.downloadContentCount === 1)) {
      this.interrupt = false;
      this.observer.next(this.contentDownloadData);    
      _.forIn(this.contentDownloadMetaData.contentDownloadList, (value: IContentDownloadList, key) => {
        if (value.step === "DOWNLOAD") {
          const cancelRes = this.downloadSDK.cancel(value.downloadId);
        }
      });
      this.observer.error({
        code: _.get(error, 'code') || "DOWNLOAD_FILE_FAILED",
        status: 400,
        message: `More than one content download failed`,
      });
    } else { // complete download task if all are completed
      this.checkForAllTaskCompletion();
    }
  }
  private handleDownloadProgress(contentId: string, progress: IDownloadProgress) {
    const contentDetails = this.contentDownloadMetaData.contentDownloadList[contentId];
    const downloadedSize = this.contentDownloadMetaData.downloadedSize
      + (contentDetails.size * (progress.total.percentage / 100));
    this.contentDownloadData.progress = downloadedSize;
    this.observer.next(this.contentDownloadData);
  }

  private async handleDownloadComplete(contentId, contentDetails: IContentDownloadList) {
    try {
      if (this.interrupt) {
        return;
      }
      if (!_.includes(["EXTRACT", "INDEX", "COMPLETE", "DELETE"], contentDetails.step)) {
        throw new Error("INVALID_STEP");
      }
      let itemsToDelete = [];
      if (contentDetails.step === "EXTRACT") {
        const res = await this.extractContent(contentId, contentDetails);
        itemsToDelete = res.itemsToDelete || [];
        contentDetails.step = "INDEX";
        this.observer.next(this.contentDownloadData);
      }
      if (this.interrupt) {
        return;
      }
      if (contentDetails.step === "INDEX") {
        await this.saveContentToDb(contentId, contentDetails);
        contentDetails.step = "COMPLETE";
        this.observer.next(this.contentDownloadData);
      }
      if (this.interrupt) {
        return;
      }
      for (const item of itemsToDelete) {
        await this.fileSDK.remove(item).catch((error) => {
          logger.error(`Received error while deleting content path: ${path} and error: ${error}`);
        });
      }
      this.checkForAllTaskCompletion();
    } catch (err) {
      logger.error(`${this.contentDownloadData._id}:error while processing download complete event: ${contentId}`,
        err.message);
      this.extractionFailedCount += 1;
      if (this.extractionFailedCount > 1 || (this.downloadContentCount === 1)) {
        this.interrupt = false;
        this.observer.next(this.contentDownloadData);
        this.observer.error({
          code: "CONTENT_EXTRACT_FAILED",
          status: 400,
          message: `More than one content extraction after download failed`,
        });
      }
    }
  }
  private async extractZipEntry(zipHandler, entry: string, distFolder): Promise<boolean | any> {
    return new Promise(async (resolve, reject) => zipHandler.extract(entry,
      distFolder, (err) => err ? reject(err) : resolve()));
  }
  private async extractContent(contentId, contentDetails: IContentDownloadList) {
    const itemsToDelete = [];
    logger.debug(`${this.contentDownloadData._id}:Extracting content: ${contentId}`);
    const zipHandler: any = await this.loadZipHandler(path.join(this.ecarBasePath, contentDetails.downloadId));
    await this.checkSpaceAvailability(path.join(this.ecarBasePath, contentDetails.downloadId), zipHandler);
    const entries = zipHandler.entries();
    await this.fileSDK.mkdir(path.join("content", contentDetails.identifier));
    for (const entry of _.values(entries) as any) {
      await this.extractZipEntry(zipHandler, entry.name,
        path.join(this.fileSDK.getAbsPath("content"), contentDetails.identifier));
    }
    zipHandler.close();
    logger.debug(`${this.contentDownloadData._id}:Extracted content: ${contentId}`);
    itemsToDelete.push(path.join("ecars", contentDetails.downloadId));
    const manifestJson = await this.fileSDK.readJSON(
      path.join(this.fileSDK.getAbsPath("content"), contentDetails.identifier, "manifest.json"));
    const metaData: any = _.get(manifestJson, "archive.items[0]");
    if (_.endsWith(metaData.artifactUrl, ".zip")) {
      await this.checkSpaceAvailability(path.join(this.fileSDK.getAbsPath("content"),
        contentDetails.identifier, path.basename(metaData.artifactUrl)));
      logger.debug(`${this.contentDownloadData._id}:Extracting artifact url content: ${contentId}`);
      await this.fileSDK.unzip(path.join("content", contentDetails.identifier, path.basename(metaData.artifactUrl)),
        path.join("content", contentDetails.identifier), false);
      itemsToDelete.push(path.join("content", contentDetails.identifier, path.basename(metaData.artifactUrl)));
    }
    contentDetails.step = "EXTRACT";
    this.observer.next(this.contentDownloadData);
    return { itemsToDelete };
  }
  private async saveContentToDb(contentId: string, contentDetails: IContentDownloadList) {
    logger.debug(`${this.contentDownloadData._id}:Saving content: ${contentId} in database`);
    const manifestJson = await this.fileSDK.readJSON(
      path.join(this.fileSDK.getAbsPath("content"), contentDetails.identifier, "manifest.json"));
    const metaData: any = _.get(manifestJson, "archive.items[0]");
    if (metaData.mimeType === "application/vnd.ekstep.content-collection") {
      metaData.children = this.createHierarchy(_.cloneDeep(_.get(manifestJson, "archive.items")), metaData);
    }
    metaData.baseDir = `content/${contentDetails.identifier}`;
    metaData.desktopAppMetadata = {
      "addedUsing": ContentDownloader.taskType,
      "createdOn": Date.now(),
      "updatedOn": Date.now(),
      "isAvailable": true,
    };
    if (contentId !== this.contentDownloadMetaData.contentId) {
      metaData.visibility = "Parent";
    }
    await this.databaseSdk.upsert("content", metaData.identifier, metaData);
    contentDetails.step = "INDEX";
    this.observer.next(this.contentDownloadData);
  }
  private checkForAllTaskCompletion() {
    let totalContents = 0;
    let completedContents = 0;
    _.forIn(this.contentDownloadMetaData.contentDownloadList, (value, key) => {
      totalContents += 1;
      if (value.step === "COMPLETE" || value.step === "DELETE") { // delete content will be done async 
        completedContents += 1;
      }
    });
    if (totalContents === (completedContents + this.extractionFailedCount + this.downloadFailedCount)) {
      logger.debug(`${this.contentDownloadData._id}:download completed`);
      this.constructShareEvent(this.contentDownloadData);
      this.deleteRemovedContent();
      this.observer.complete();
    } else {
      logger.debug(`${this.contentDownloadData._id}:Extraction completed for ${completedContents},
      ${totalContents - completedContents}`);
    }
  }
  private constructShareEvent(data) {
    const telemetryShareItems = [{
      id: _.get(data, "metaData.contentId"),
      type: _.get(data, "metaData.contentType"),
      ver: _.toString(_.get(data, "metaData.pkgVersion")),
    }];
    this.telemetryHelper.logShareEvent(telemetryShareItems, "In", "Content");
}
  private deleteRemovedContent(){ // if content has been removed from collection make the content visibility to default
    const updateDoc = [];
    _.forIn(this.contentDownloadMetaData.contentDownloadList, (value: IContentDownloadList, key) => {
      if (value.step === "DELETE") {
        updateDoc.push({
          _id: value.identifier,
          identifier: value.identifier,
          visibility: "Default",
          updatedOn: Date.now()
        });
      }
    });
    if(updateDoc.length){
      this.databaseSdk.bulk("content", updateDoc).catch(error => {
        logger.debug(`${this.contentDownloadData._id}: content visibility update failed for deleted content`, error.message)
      });
    }
  }
  private async checkSpaceAvailability(zipPath, zipHandler?) {
    let closeZipHandler = true;
    if (zipHandler) {
      closeZipHandler = false;
    }
    zipHandler = zipHandler || await this.loadZipHandler(zipPath);
    const entries = zipHandler.entries();
    const availableDiskSpace = await this.getAvailableDiskSpace();
    let contentSize = 0; // size in bytes
    for (const entry of _.values(entries) as any) {
      contentSize += entry.size;
    }
    if (contentSize > availableDiskSpace) {
      throw { message: "Disk space is low, couldn't extract Ecar", code: "LOW_DISK_SPACE" };
    }
    if (closeZipHandler) {
        zipHandler.close();
    }
  }
  private async loadZipHandler(filePath) {
    const zip = new StreamZip({ file: filePath, storeEntries: true, skipEntryNameValidation: true });
    return new Promise((resolve, reject) => {
      zip.on("ready", () => resolve(zip));
      zip.on("error", reject);
    });
  }
  private async getAvailableDiskSpace() {
    return this.systemSDK.getHardDiskInfo().then(({ availableHarddisk, fsSize }) => {
      if (os.platform() === "win32") {
        const fileSize: any = fsSize;
        const totalHarddisk = _.find(fileSize, { mount: "C:" })["size"] || 0;
        const usedHarddisk = _.find(fileSize, { mount: "C:" })["used"] || 0;
        const availableHardDisk = totalHarddisk - usedHarddisk;
        return availableHardDisk - 3e+8; // keeping buffer of 300 mb, this can be configured
      } else {
        return availableHarddisk - 3e+8; // keeping buffer of 300 mb, this can be configured
      }
    });
  }
  private createHierarchy(items: any[], parent: any, tree?: any[]): any {
    tree = typeof tree !== "undefined" ? tree : [];
    parent = typeof parent !== "undefined" ? parent : { visibility: "Default" };
    if (parent.children && parent.children.length) {
      let children = [];
      _.forEach(items, (child) => {
        const childWithIndex: any = _.find(parent.children, { identifier: child.identifier });
        if (!_.isEmpty(childWithIndex)) {
          child.index = childWithIndex.index;
          children.push(child);
        }
      });
      if (!_.isEmpty(children)) {
        children = _.sortBy(children, "index");
        if (parent.visibility === "Default") {
          tree = children;
        } else {
          parent.children = children;
        }
        _.each(children, (child) => this.createHierarchy(items, child));
      }
    }
    return tree;
  }
}
interface IDownloadMetaData {
  url: string;
  savePath: string;
  sudPath: string;
  filesize: number;
  ranges: any[];
}

interface IDownloadProgress {
  time: { start: number, elapsed: number, eta: number };
  total: {
    filesize: number;
    downloaded: number;
    percentage: number;
  };
  instance: { downloaded: number; percentage: number };
  speed: number;
  avgSpeed: number;
  threadPositions: number[];
}
