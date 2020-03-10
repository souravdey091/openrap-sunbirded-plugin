import { logger } from "@project-sunbird/ext-framework-server/logger";
import * as _ from "lodash";
import { containerAPI, ISystemQueue, ITaskExecuter } from "OpenRAP/dist/api";
import { Observer, of } from "rxjs";
import { retry } from "rxjs/operators";
import { manifest } from "../../manifest";


export class ContentDeleteHelper implements ITaskExecuter {
  public static taskType = "DELETE";
  public concurrency = 1;
  public queue = [];
  public running = 0;
  private contentDeleteData;
  private observer: Observer<ISystemQueue>;
  private systemQueue = containerAPI.getSystemQueueInstance(manifest.id);
  private fileSDK = containerAPI.getFileSDKInstance(manifest.id);

  public async start(contentDeleteData: ISystemQueue, observer: import("rxjs").Observer<ISystemQueue>) {
    this.observer  = observer;
    _.forEach(contentDeleteData.metaData.filePaths, (filePath) => {
      this.pushToQueue(filePath);
    });
    return true;
  }

  public status(): ISystemQueue {
    return this.contentDeleteData;
  }

  public pushToQueue(filePath) {
    if (this.checkPath(filePath)) {
        this.queue.push(filePath);
        this.next();
    }
  }

  private next() {
    while (this.queue.length) {
        const filePath = this.queue.shift();
        const deleteSub = of(this.fileSDK.remove(filePath)).pipe(retry(5));
        const deleteSubscription = deleteSub.subscribe({
                next: (val) => {
                    if (this.queue.length === 0) {
                      this.observer.complete();
                    }
                },
                error: (err) => {
                    this.observer.error(err);
                    logger.error(`error while deleting the content ${err.stack} and retried for 5 times`);
                },
              });
    }
  }
  private checkPath(filePath) {
    const regex = /^content/i;
    return filePath.match(regex) && !_.includes(this.queue, filePath);
  }
}
