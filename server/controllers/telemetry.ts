import { logger } from "@project-sunbird/ext-framework-server/logger";
import { Manifest } from "@project-sunbird/ext-framework-server/models";
import * as _ from "lodash";
import { containerAPI } from "OpenRAP/dist/api";
import { Inject } from "typescript-ioc";
import DatabaseSDK from "../sdk/database";
import Response from "./../utils/response";

export default class Telemetry {
  @Inject
  private databaseSdk: DatabaseSDK;
  private telemetrySDK;

  constructor(manifest: Manifest) {
    this.databaseSdk.initialize(manifest.id);
    this.telemetrySDK = containerAPI.getTelemetrySDKInstance();
  }

  public addEvents(req, res) {
    logger.debug(
      `ReqId = "${req.headers["X-msgid"]}": Called telemetry addEvents method`,
    );
    logger.info(
      `ReqId = "${req.headers["X-msgid"]}": adding telemetry events: ${req.body.events.length}`,
    );
    const events = req.body.events;
    if (_.isArray(events) && events.length) {
      logger.debug(
        `ReqId = "${req.headers["X-msgid"]}": telemetry service is called to add telemetryEvents`,
      );
      this.telemetrySDK
        .send(events)
        .then((data) => {
          logger.info(
            `ReqId = "${req.headers["X-msgid"]}": Telemetry events added successfully ${data}`,
          );
          return res.send(Response.success("api.telemetry", {}, req));
        })
        .catch((err) => {
          logger.error(
            `ReqId = "${req.headers["X-msgid"]}": Received error while inserting events to telemetry db and err.message: ${err.message} `,
          );
          res.status(500);
          return res.send(Response.error("api.telemetry", 500));
        });
    } else {
      logger.error(
        `ReqId = "${req.headers["X-msgid"]}": Received err and err.res.status: 400`,
      );
      res.status(400);
      return res.send(Response.error("api.telemetry", 400));
    }
  }

  public getTelemetryInfo(req, res) {
    try {
      const resObj = {
        lastSharedDate: Date.now(),
        size: `500KB`,
      };
      return res.send(Response.success("api.telemetry.info", resObj, req));
    } catch (error) {
      res.status(500);
      return res.send(Response.error("api.telemetry.info", 500, error.message || ""));
    }
  }

  public exportTelemetry(req, res) {
    try {
      return res.send(Response.success("api.telemetry.export", {}, req));
    } catch (error) {
      res.status(500);
      return res.send(Response.error("api.telemetry.export", 500,  error.message || ""));
    }
  }

}
