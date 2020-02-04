import { logger } from "@project-sunbird/ext-framework-server/logger";
import Response from "./../utils/response";
export default class Tenant {
  public get(req, res) {
    logger.debug(`ReqId = "${req.headers["X-msgid"]}": Getting Tenant Info`);
    const resObj = {
      appLogo: "/appLogo.png",
      favicon: "/favicon.ico",
      logo: "/logo.svg",
      titleName: process.env.APP_NAME,
    };
    logger.info(`ReqId = "${req.headers["X-msgid"]}": Received Tenant Info`);
    res.send(Response.success("api.tenant.info", resObj, req));
  }
}
