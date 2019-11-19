import { ILocation } from './ILocation';
import DatabaseSDK from '../sdk/database/index';
import { Inject } from 'typescript-ioc';
import * as fs from 'fs';
import * as path from 'path';
import { Manifest } from '@project-sunbird/ext-framework-server/models';
import * as glob from 'glob';
import * as _ from 'lodash';
import Response from './../utils/response';
import { logger } from '@project-sunbird/ext-framework-server/logger';
import { containerAPI } from 'OpenRAP/dist/api';
import { HTTPService } from '@project-sunbird/ext-framework-server/services';

export class Location {
    @Inject private databaseSdk: DatabaseSDK;

    private fileSDK;
    constructor(manifest: Manifest) {
        this.databaseSdk.initialize(manifest.id);
        this.fileSDK = containerAPI.getFileSDKInstance(manifest.id);
    }

    // Inserting states and districts data from files

    public async insert() {
        logger.debug(`Location insert method is called`);
        try {
            let filesPath = this.fileSDK.getAbsPath(path.join('data', 'location', '/'));
            let stateFile = await this.fileSDK.readJSON(filesPath + 'state.json');
            let states = _.get(stateFile, 'result.response');
            let allStates: Array<ILocation> = [];
            let allDocs = await this.databaseSdk.list('location', { include_docs: true, startkey: 'design_\uffff' });
            if (allDocs.rows.length === 0) {
                for (let state of states) {
                    state._id = state.id;
                    let districtFile = await this.fileSDK.readJSON(filesPath + 'district-' + state._id + '.json');
                    state['data'] = _.get(districtFile, 'result.response') || [];
                    allStates.push(state);
                }
                logger.debug('Inserting location data in locationDB')
                await this.databaseSdk.bulk('location', allStates).catch(err => {
                    logger.error(`while inserting location data in locationDB  ${err}`);
                });
            }
            return;
        } catch (err) {
            logger.error(`while inserting location data in locationDB  ${err}`);
            return;
        }
    }

    // Searching location data in DB (if user is in online get online data and insert in db)
    async search(req, res) {
        logger.debug(`ReqId = '${req.headers['X-msgid']}': Location search method is called`);
        let locationType = _.get(req.body, 'request.filters.type');
        let parentId = _.get(req.body, 'request.filters.parentId');
        logger.debug(`ReqId = '${req.headers['X-msgid']}': Finding the data from location database`);
        if (_.isEmpty(locationType)) {
            res.status(400);
            return res.send(Response.error('api.location.read', 400, 'location Type is missing'));
        }
        if (locationType === 'district' && _.isEmpty(parentId)) {
            logger.error(
                `ReqId = '${req.headers[
                'X-msgid'
                ]}': Error Received while searching ${req.body} data error`
            );
            res.status(400);
            return res.send(Response.error('api.location.read', 400, 'parentId is missing'));
        }

        logger.debug(`ReqId = ${req.headers['X-msgid']}: getLocationData method is calling`);
        const request = _.isEmpty(parentId) ? { selector: {} } : { selector: { id: parentId } };
        await this.databaseSdk.find('location', request).then(response => {
            response = _.map(response['docs'], (doc) => locationType === 'state' ? _.omit(doc, ['_id', '_rev', 'data']) : doc.data);
            let resObj = {
                response: locationType === 'district' ? response[0] : response
            };
            logger.info(`ReqId =  ${req.headers['X-msgid']}: got data from db`);
            return res.send(Response.success('api.location.read', resObj, req));
        }).catch(err => {
            logger.error(
                `ReqId = "${req.headers[
                'X-msgid'
                ]}": Received error while searching in location database and err.message: ${err.message} ${err}`
            );
            if (err.status === 404) {
                res.status(404);
                return res.send(Response.error('api.location.read', 404));
            } else {
                let status = err.status || 500;
                res.status(status);
                return res.send(Response.error('api.location.read', status));
            }
        });
    }
    async proxyToAPI(req, res, next) {

        let requestObj = {
            type: _.get(req.body, 'request.filters.type'),
            parentId: _.get(req.body, 'request.filters.parentId')
        }

        const config = {
            headers: {
                authorization: `Bearer ${process.env.APP_BASE_URL_TOKEN}`,
                'content-type': 'application/json',
            },
        };
        const filter = _.isEmpty(requestObj.parentId)
            ? { filters: { type: requestObj.type } }
            : { filters: { type: requestObj.type, parentId: requestObj.parentId } };

        const requestParams = {
            request: filter,
        };
        try {
            logger.debug(`ReqId =  ${req.headers["X-msgid"]}}: getting location data from online`);
            let responseData = await HTTPService.post(
                `${process.env.APP_BASE_URL}/api/data/v1/location/search`,
                requestParams,
                config
            ).toPromise();

            let response = _.get(responseData.data, 'result.response')
            requestObj.type === 'state' ? await this.insertStatesDataInDB(response, req.headers["X-msgid"]) : await this.updateStateDataInDB(response, req.headers["X-msgid"]);
            response = _.map(response, data => _.omit(data, ['_id', 'data']));
            let resObj = {
                response: response
            }
            
            logger.debug(`ReqId =  ${req.headers["X-msgid"]}: fetchLocationFromOffline method is calling `)
            return res.send(Response.success('api.location.read', resObj, req));
        } catch (err) {
            logger.error(`ReqId =  ${req.headers["X-msgid"]}: Error Received while getting data from Online ${err}`)
            next();
        }
    }

    async insertStatesDataInDB(onlineStates, msgId) {
        logger.debug(`ReqId =  ${msgId}: insertStatesDataInDB method is called `)

        try {
            let bulkInsert = [], bulkUpdate = [];
            let allDocs = await this.databaseSdk.find('location', { selector: {} });
            let ids = _.map(allDocs.docs, doc => { doc.id });
            if (!_.isEmpty(onlineStates)) {
                for (let state of onlineStates) {
                    if (_.includes(ids, state.id)) {
                        state._id = state.id;
                        bulkUpdate.push(state);
                    } else {
                        state._id = state.id;
                        _.has(state, 'data') ? state.data = state['data'] : state['data'] = []
                        bulkInsert.push(state);
                    }
                }
                if (bulkInsert.length > 0) {
                    logger.info(`ReqId =  ${msgId}: bulkInsert in LocationDB`)
                    await this.databaseSdk.bulk('location', bulkInsert);
                }
                if (bulkUpdate.length > 0) {
                    logger.info(`ReqId =  ${msgId}: bulkUpdate in LocationDB`)
                    await this.databaseSdk.bulk('location', bulkUpdate);
                }
            } else {
                logger.info(`ReqId =  ${msgId}: state data is empty`)
                return;
            }
        } catch(err) {
            logger.error(`ReqId =  ${msgId}: updateStateDataInDB method is called ${err}`);
            return;
        }
    }
    async updateStateDataInDB(district, msgId) {
        logger.debug(`ReqId =  ${msgId}: updateStateDataInDB method is called `)

        try {
            let id = district[0]['parentId'];
            logger.info(`ReqId =  ${msgId}: getting data from LocationDB`)
            let state = await this.databaseSdk.get('location', id);
            if (!_.isEmpty(district)) {
                state.data = district;
                logger.info(`ReqId =  ${msgId}: updating data in LocationDB`)
                await this.databaseSdk.update('location', state.id, state);
            } else {
                logger.info(`ReqId =  ${msgId}: district data is empty`)
                return;
            }

        } catch(err) {
            logger.error(`ReqId =  ${msgId}: updateStateDataInDB method is called ${err}`);
            return;
        }
    }

}
