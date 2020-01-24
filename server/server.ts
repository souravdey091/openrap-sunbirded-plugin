import {
  Manifest,
  BaseServer
} from "@project-sunbird/ext-framework-server/models";
import { frameworkAPI } from "@project-sunbird/ext-framework-server/api";
import * as path from "path";
import { Inject } from "typescript-ioc";
import {ContentImportManager} from "./manager/contentImportManager"
import { Framework } from "./controllers/framework";
import { Faqs } from "./controllers/faqs";
import { Organization } from "./controllers/organization";
import { Page } from "./controllers/page";
import { ResourceBundle } from "./controllers/resourceBundle";
import { Channel } from "./controllers/channel";
import { Form } from "./controllers/form";
import { Location } from './controllers/location';
import DatabaseSDK from "./sdk/database";
import config from "./config";
import { logger } from "@project-sunbird/ext-framework-server/logger";
import { containerAPI, ISystemQueueInstance  } from "OpenRAP/dist/api";
import  ContentDelete from "./controllers/content/contentDelete";
import {
  addContentListener,
  reconciliation
} from "./controllers/content/contentHelper";
import * as _ from "lodash";
import { EventManager } from "@project-sunbird/ext-framework-server/managers/EventManager";
import {ImportContent} from "./manager/contentImportManager/contentImport";
export class Server extends BaseServer {
  private sunbirded_plugin_initialized = false;
  private ecarsFolderPath: string = "ecars";
  private contentFilesPath: string = "content";

  @Inject
  private databaseSdk: DatabaseSDK;

  @Inject
  private contentImportManager: ContentImportManager;

  @Inject
  private fileSDK;

  @Inject
  private contentDelete: ContentDelete;

  private systemQueue: ISystemQueueInstance;
  constructor(manifest: Manifest) {
    super(manifest);
    this.systemQueue = containerAPI.getSystemQueueInstance(manifest.id);

    // Added timeout since db creation is async and it is taking time and insertion is failing
    this.fileSDK = containerAPI.getFileSDKInstance(manifest.id);

    this.initialize(manifest)
      .then(() => {
        this.sunbirded_plugin_initialized = true;
        EventManager.emit(`${manifest.id}:initialized`, {});
      })
      .catch(err => {
        logger.error(
          "Error while initializing open rap sunbird ed plugin",
          err
        );
        this.sunbirded_plugin_initialized = true;
        EventManager.emit(`${manifest.id}:initialized`, {});
      });
  }
  async initialize(manifest: Manifest) {
    this.systemQueue.register(ImportContent.taskType, ImportContent);
    //registerAcrossAllSDKS()    
    this.databaseSdk.initialize(manifest.id);
    this.contentDelete = new ContentDelete(manifest);
    frameworkAPI.registerStaticRoute(
      this.fileSDK.getAbsPath(this.contentFilesPath),
      "/contentPlayer/preview/content"
    );
    frameworkAPI.registerStaticRoute(
      this.fileSDK.getAbsPath(this.contentFilesPath),
      "/contentPlayer/preview"
    );
    frameworkAPI.registerStaticRoute(
      this.fileSDK.getAbsPath(this.contentFilesPath),
      "/contentPlayer/preview/content/*/content-plugins"
    );
    frameworkAPI.registerStaticRoute(
      path.join(__dirname, "..", "..", "public", "contentPlayer", "preview"),
      "/contentPlayer/preview"
    );
    frameworkAPI.registerStaticRoute(
      this.fileSDK.getAbsPath(this.contentFilesPath),
      "/content"
    );
    frameworkAPI.registerStaticRoute(
      this.fileSDK.getAbsPath(this.ecarsFolderPath),
      "/ecars"
    );
    frameworkAPI.registerStaticRoute(
      path.join(__dirname, "..", "..", "public", "portal")
    );
    frameworkAPI.registerStaticRoute(
      path.join(__dirname, "..", "..", "public", "sunbird-plugins"),
      "/sunbird-plugins"
    );
    frameworkAPI.setStaticViewEngine("ejs");

    // insert meta data for app
    await this.insertConfig(manifest);

    const pluginConfig = {
      pluginVer: manifest.version,
      apiToken: process.env.APP_BASE_URL_TOKEN,
      apiBaseURL: process.env.APP_BASE_URL,
      apiTokenRefreshFn: "refreshToken"
    };
    await containerAPI.register(manifest.id, pluginConfig);

    await this.fileSDK.mkdir(this.contentFilesPath);
    await this.fileSDK.mkdir(this.ecarsFolderPath);

    this.contentImportManager.initialize(
      manifest.id,
      this.fileSDK.getAbsPath(this.contentFilesPath),
      this.fileSDK.getAbsPath(this.ecarsFolderPath)
    );
    setTimeout(async () => {

      addContentListener(manifest.id);
      reconciliation(manifest.id);
      await this.contentImportManager.reconcile();
      await this.contentDelete.reconciliation();
    }, 120000);
    //- reIndex()
    //- reConfigure()
  }

  private async insertConfig(manifest: Manifest) {
    const framework = new Framework(manifest);
    const faqs = new Faqs(manifest);
    const organization = new Organization(manifest);
    const page = new Page(manifest);
    const resourceBundle = new ResourceBundle(manifest);
    const channel = new Channel(manifest);
    const form = new Form(manifest);
    const location = new Location(manifest);
    Promise.all([organization.insert(), resourceBundle.insert(),
      framework.insert(), faqs.insert(),
      channel.insert(), form.insert(),
      form.insert(), page.insert(), location.insert()]);
  }
}

process
  .on("unhandledRejection", (reason, p) => {
    logger.error(reason, "Unhandled Rejection at Promise", p);
  })
  .on("uncaughtException", err => {
    logger.error(err, "Uncaught Exception thrown");
    process.exit(1);
  });
