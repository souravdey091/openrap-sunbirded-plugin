import { Inject } from "typescript-ioc";
import DatabaseSDK from "../../sdk/database";
import * as _ from "lodash";
import Response from "../../utils/response";
import { Manifest } from "@project-sunbird/ext-framework-server/models";
import { logger } from "@project-sunbird/ext-framework-server/logger";
import { containerAPI } from "OpenRAP/dist/api";
import { HTTPService } from "@project-sunbird/ext-framework-server/services";
import { CONTENT_DOWNLOAD_STATUS } from "./contentDownload";
import * as path from "path";

let dbName = "content_download";
export default class ContentUpdate {
  @Inject
  private databaseSdk: DatabaseSDK;
  private pluginId;

  constructor(manifest: Manifest) {
    this.databaseSdk.initialize(manifest.id);
    this.pluginId = manifest.id;
  }

  async contentUpdate(req: any, res: any) {
    try {
      let id = req.params.id;
      let parentId = _.get(req.body, "request.parentId");
      const localContentData = await this.databaseSdk.get("content", id);
      if (!_.has(localContentData.desktopAppMetadata, "isAvailable") ||
      localContentData.desktopAppMetadata.isAvailable) {
        let liveContentData = await HTTPService.get(
          `${process.env.APP_BASE_URL}/api/content/v1/read/${id}`,
          {}
        ).toPromise();
  
        if (
          parentId &&
          _.get(liveContentData, "data.result.content.mimeType") !==
            "application/vnd.ekstep.content-collection" &&
          _.get(liveContentData, "data.result.content.pkgVersion") >
            localContentData.pkgVersion
        ) {
          // Resource update inside collection
          logger.debug(
            `Resource Id inside collection = "${id}" for content update`
          );
          await this.resourceInsideCollectionUpdate(
            parentId,
            liveContentData
          ).then(data => {
            return res.send(Response.success("api.content.update", data, req));
          });
        } else if (
          _.get(liveContentData, "data.result.content.mimeType") ===
            "application/vnd.ekstep.content-collection" &&
          _.get(liveContentData, "data.result.content.pkgVersion") >
            localContentData.pkgVersion
        ) {
          // Collection update
          logger.debug(`Collection Id = "${id}" for content update`);
          await this.collectionUpdate(localContentData, liveContentData).then(
            data => {
              return res.send(Response.success("api.content.update", data, req));
            }
          );
        } else if (
          _.get(liveContentData, "data.result.content.mimeType") !==
            "application/vnd.ekstep.content-collection" &&
          _.get(liveContentData, "data.result.content.pkgVersion") >
            localContentData.pkgVersion
        ) {
          // Resource update
          logger.debug(`Resource Id = "${id}" for content update`);
          await this.resourceUpdate(liveContentData).then(data => {
            return res.send(Response.success("api.content.update", data, req));
          });
        } else {
          logger.error(
            `ReqId = "${req.headers["X-msgid"]}": Update not available for the content: ${req.params.id}`
          );
          res.status(400);
          return res.send(
            Response.error("api.content.update", 400, "Update not available")
          );
        }
      } else {
        res.status(404);
        return res.send(
          Response.error("api.content.update", status, "Content Not Found")
        );
      }
    } catch (error) {
      logger.error(
        `ReqId = "${
          req.headers["X-msgid"]
        }": Received error while processing content update for the content: ${
          req.params.id
        } and error = ${JSON.stringify(error)}`
      );
      let status = error.status ? error.status : 500;
      res.status(status);
      return res.send(
        Response.error("api.content.update", status, error.message)
      );
    }
  }

  resourceInsideCollectionUpdate(parentId, liveContentData) {
    return new Promise(async (resolve, reject) => {
      try {
        let downloadManager = containerAPI.getDownloadManagerInstance(
          this.pluginId
        );
        let parentContentData = await HTTPService.get(
          `${process.env.APP_BASE_URL}/api/content/v1/read/${parentId}`,
          {}
        ).toPromise();

        let downloadFiles = [
          {
            id: _.get(
              liveContentData,
              "data.result.content.identifier"
            ) as string,
            url: _.get(
              liveContentData,
              "data.result.content.downloadUrl"
            ) as string,
            size: _.get(liveContentData, "data.result.content.size") as number
          }
        ];
        let downloadId = await downloadManager.download(downloadFiles, "ecars");
        let queueMetaData = {
          mimeType: _.get(parentContentData, "data.result.content.mimeType"),
          items: downloadFiles,
          pkgVersion: _.get(
            parentContentData,
            "data.result.content.pkgVersion"
          ),
          contentType: _.get(
            parentContentData,
            "data.result.content.contentType"
          ),
          resourceId: _.get(liveContentData, "data.result.content.identifier")
        };
        logger.debug(
          `Resource inside collection insert to the content_download for content update`
        );
        await this.databaseSdk.insert(dbName, {
          downloadId: downloadId,
          contentId: _.get(parentContentData, "data.result.content.identifier"),
          name: _.get(parentContentData, "data.result.content.name"),
          status: CONTENT_DOWNLOAD_STATUS.Submitted,
          queueMetaData: queueMetaData,
          createdOn: Date.now(),
          updatedOn: Date.now()
        });
        logger.info(
          `Resource inserted in database successfully for content update`
        );
        resolve(downloadId);
      } catch (err) {
        reject(err);
      }
    });
  }

  collectionUpdate(localContentData, liveContentData) {
    return new Promise(async (resolve, reject) => {
      try {
        let downloadManager = containerAPI.getDownloadManagerInstance(
          this.pluginId
        );
        let downloadFiles = [
          {
            id: _.get(
              liveContentData,
              "data.result.content.identifier"
            ) as string,
            url: _.get(
              liveContentData,
              "data.result.content.downloadUrl"
            ) as string,
            size: _.get(liveContentData, "data.result.content.size") as number
          }
        ];

        // Get the local child contents
        const localChildNodes = _.get(localContentData, "childNodes");
        if (!_.isEmpty(localChildNodes)) {
          let { docs: localChildContents = [] } = await this.databaseSdk.find(
            "content",
            {
              selector: {
                $and: [
                  {
                    _id: {
                      $in: localChildNodes
                    }
                  },
                  {
                    mimeType: {
                      $nin: ["application/vnd.ekstep.content-collection"]
                    }
                  }
                ]
              }
            }
          );

          // Get the live child contents
          let liveChildNodes = _.get(
            liveContentData,
            "data.result.content.childNodes"
          );
          if (!_.isEmpty(liveChildNodes)) {
            let liveChildrenContentsRes = await HTTPService.post(
              `${process.env.APP_BASE_URL}/api/content/v1/search`,
              {
                request: {
                  filters: {
                    identifier: liveChildNodes,
                    mimeType: {
                      "!=": "application/vnd.ekstep.content-collection"
                    }
                  },
                  limit: liveChildNodes.length
                }
              },
              {
                headers: {
                  "Content-Type": "application/json"
                }
              }
            ).toPromise();

            if (_.get(liveChildrenContentsRes, "data.result.count")) {
              let liveChildContents = _.get(
                liveChildrenContentsRes,
                "data.result.content"
              );
              let deletedObj = this.getDeletedContents(
                localChildContents,
                liveChildContents
              );
              let addedAndUpdatedObj = this.getAddedAndUpdatedContents(
                liveChildContents,
                localChildContents
              );

              // Updating visibilty to Default for deleted resources
              _.forEach(deletedObj, content => {
                content.visibility = "Default";
                const imageName = path.basename(content.appIcon);
                content.appIcon = path.join(
                  "content",
                  content.identifier,
                  content.identifier,
                  imageName
                );
                content.updatedOn = Date.now();
              });
              await this.databaseSdk.bulk("content", localChildContents);

              // Pushing downloadable childs to downloadfiles array
              _.forEach(addedAndUpdatedObj, content => {
                downloadFiles.push({
                  id: _.get(content, "identifier"),
                  url: _.get(content, "downloadUrl"),
                  size: _.get(content, "size")
                });
              });
            }
          }
        }

        let downloadId = await downloadManager.download(downloadFiles, "ecars");
        let queueMetaData = {
          mimeType: _.get(liveContentData, "data.result.content.mimeType"),
          items: downloadFiles,
          pkgVersion: _.get(liveContentData, "data.result.content.pkgVersion"),
          contentType: _.get(
            liveContentData,
            "data.result.content.contentType"
          ),
          resourceId: _.get(liveContentData, "data.result.content.identifier")
        };
        await this.databaseSdk.insert(dbName, {
          downloadId: downloadId,
          contentId: _.get(liveContentData, "data.result.content.identifier"),
          name: _.get(liveContentData, "data.result.content.name"),
          status: CONTENT_DOWNLOAD_STATUS.Submitted,
          queueMetaData: queueMetaData,
          createdOn: Date.now(),
          updatedOn: Date.now()
        });
        logger.info(
          `Collection inserted in database successfully for content update`
        );
        resolve(downloadId);
      } catch (err) {
        reject(err);
      }
    });
  }

  getAddedAndUpdatedContents(liveContents, localContents) {
    const contents = _.filter(liveContents, data => {
      const b = _.find(localContents, {
        _id: data.identifier,
        pkgVersion: data.pkgVersion
      });
      return b ? false : true;
    });
    return contents;
  }

  getDeletedContents(localContents, liveContents) {
    const contents = _.filter(localContents, data => {
      const b = _.find(liveContents, { identifier: data._id });
      return b ? false : true;
    });
    return contents;
  }

  resourceUpdate(liveContentData) {
    return new Promise(async (resolve, reject) => {
      try {
        let downloadManager = containerAPI.getDownloadManagerInstance(
          this.pluginId
        );
        let downloadFiles = [
          {
            id: _.get(
              liveContentData,
              "data.result.content.identifier"
            ) as string,
            url: _.get(
              liveContentData,
              "data.result.content.downloadUrl"
            ) as string,
            size: _.get(liveContentData, "data.result.content.size") as number
          }
        ];
        let downloadId = await downloadManager.download(downloadFiles, "ecars");
        let queueMetaData = {
          mimeType: _.get(liveContentData, "data.result.content.mimeType"),
          items: downloadFiles,
          pkgVersion: _.get(liveContentData, "data.result.content.pkgVersion"),
          contentType: _.get(
            liveContentData,
            "data.result.content.contentType"
          ),
          resourceId: _.get(liveContentData, "data.result.content.identifier")
        };
        logger.debug(
          `Resource insert to the content_download for content update`
        );
        await this.databaseSdk.insert(dbName, {
          downloadId: downloadId,
          contentId: _.get(liveContentData, "data.result.content.identifier"),
          name: _.get(liveContentData, "data.result.content.name"),
          status: CONTENT_DOWNLOAD_STATUS.Submitted,
          queueMetaData: queueMetaData,
          createdOn: Date.now(),
          updatedOn: Date.now()
        });
        logger.info(
          `Resource inserted in database successfully for content update`
        );
        resolve(downloadId);
      } catch (err) {
        reject(err);
      }
    });
  }
}
