import { logger } from "@project-sunbird/ext-framework-server/logger";
import { HTTPService } from "@project-sunbird/ext-framework-server/services";
import * as  _ from "lodash";
import { containerAPI, ISystemQueue, ITaskExecuter } from "OpenRAP/dist/api";
import { NetworkQueue } from "OpenRAP/dist/services/queue";
import { Observer } from "rxjs";
import { of, throwError } from "rxjs";
import { catchError, mergeMap, retry } from "rxjs/operators";
const successResponseCode = ["success", "ok"];

export class NetworkQueueForceSync implements ITaskExecuter {
  public static taskType = "NETWORK_QUEUE_FORCE_SYNC";
  private forceSyncData: ISystemQueue;
  private observer: Observer<ISystemQueue>;
  private networkQueue: NetworkQueue;
  private apiKey: string;
  private concurrency: number = 6;
  constructor() {
    this.networkQueue = containerAPI.getNetworkQueueInstance();
  }

  public status() {
    return this.forceSyncData;
  }

  public async start(forceSyncData: ISystemQueue, observer: Observer<ISystemQueue>) {
    this.apiKey = process.env.APP_BASE_URL_TOKEN;
    // tslint:disable-next-line:no-console
    console.log("=============================================", this.apiKey);
    logger.debug("Force network queue sync task executor initialized for ", forceSyncData);
    this.forceSyncData = forceSyncData;
    this.observer = observer;
    this.getQuery();
    return true;
  }

  public async getQuery() {
    const query = {
      selector: {
        subType: { $in: this.forceSyncData.metaData.subType },
      },
      limit: this.concurrency,
    };

    const dbData = await this.networkQueue.getByQuery(query);
    if (!dbData || dbData.length === 0) {
      logger.info("All network queue data is synced");
      this.observer.complete();
      return;
    }
    await this.executeForceSync(dbData);
  }

  private async executeForceSync(dbData) {
    for (const currentQueue of dbData) {
      // tslint:disable-next-line:no-string-literal
      currentQueue.requestHeaderObj["Authorization"] = currentQueue.bearerToken ? `Bearer ${this.apiKey}` : "";
      const requestBody = _.get(currentQueue, "requestHeaderObj.Content-Encoding") === "gzip" ?
        Buffer.from(currentQueue.requestBody.data) : currentQueue.requestBody;
      try {
        const resp = await this.makeHTTPCall(currentQueue.requestHeaderObj, requestBody, currentQueue.pathToApi);
        if (_.includes(successResponseCode, _.toLower(_.get(resp, "data.responseCode")))) {
          logger.info(`Network Queue synced for id = ${currentQueue._id}`);
          await this.networkQueue.deQueue(currentQueue._id).catch((error) => {
            logger.info(`Received error deleting id = ${currentQueue._id}`);
          });
        } else {
          const error = {
            code: _.get(resp, "response.statusText"),
            status: _.get(resp, "response.status"),
            message: _.get(resp, "response.data.message"),
          };
          logger.error(this.forceSyncData._id, "Error while syncing to Network Queue for id ", error);
          await this.networkQueue.deQueue(currentQueue._id).catch((err) => {
            logger.info(`Received error deleting id = ${currentQueue._id}, error: ${err}`);
          });
          this.observer.next(this.forceSyncData);
          this.observer.error(error);
          return;
        }
      } catch (error) {
        logger.error(this.forceSyncData._id, "Error while syncing to Network Queue for id ", error);
        this.observer.next(this.forceSyncData);
        this.observer.error(error);
        return;
      }
    }
    await this.getQuery();
  }

  private async makeHTTPCall(headers: object, body: object, pathToApi: string) {
    return await HTTPService.post(
      pathToApi,
      body,
      { headers },
    ).pipe(mergeMap((data) => {
      return of(data);
    }), catchError((error) => {
      if (_.get(error, "response.status") >= 500 && _.get(error, "response.status") < 599) {
        return throwError(error);
      } else {
        return of(error);
      }
    }), retry(5)).toPromise();
  }
}
