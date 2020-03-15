import { logger } from "@project-sunbird/ext-framework-server/logger";
import { containerAPI, ISystemQueueInstance, SystemQueueReq } from "OpenRAP/dist/api";
import { Singleton } from "typescript-ioc";
import { manifest } from "../../manifest";
import { NetworkQueueForceSync } from "./networkQueueForceSync";

@Singleton
export class NetworkQueueForceSyncManager {
  private systemQueue: ISystemQueueInstance;

  public async initialize() {
    this.systemQueue = containerAPI.getSystemQueueInstance(manifest.id);
    this.systemQueue.register(NetworkQueueForceSync.taskType, NetworkQueueForceSync);
  }

  public async add(subType: string[]): Promise<string[]> {
    logger.info("Force network queue started: ", subType);
    const insertData: SystemQueueReq = {
      type: NetworkQueueForceSync.taskType,
      name: "Force network queue",
      metaData: { subType },
    };
    logger.info("Force network queue added to system queue", insertData);
    const id = await this.systemQueue.add(insertData);
    return id;
  }
}
