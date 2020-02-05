import * as childProcess from "child_process";
import * as _ from "lodash";
import { containerAPI, ISystemQueue, ITaskExecuter } from "OpenRAP/dist/api";
import * as path from "path";
import { Observer } from "rxjs";
import { manifest } from "../../manifest";



export class ContentDeleteHelper implements ITaskExecuter {
  public static taskType = "DELETE";
  private workerProcessRef: childProcess.ChildProcess;
  private contentDeleteData;
  private observer: Observer<ISystemQueue>;
  private systemQueue = containerAPI.getSystemQueueInstance(manifest.id);
  public async start(contentDeleteData: ISystemQueue, observer: import("rxjs").Observer<ISystemQueue>) {
    this.workerProcessRef = childProcess.fork(path.join(__dirname, "contentDeleteProcess"));
    this.observer  = observer;
    this.workerProcessRef.send(contentDeleteData.metaData.filePaths);
    this.workerProcessRef.on("message", () => {
      this.observer.complete();
    })
    return true;
  }
  public status(): ISystemQueue {
    return this.contentDeleteData;
  }
}
