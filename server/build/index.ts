import axios from "axios";
import * as fse from "fs-extra";
import * as path from "path";
import config from "./config";

const filesPath = path.join(__dirname, "..", "..", "data");
const baseUrl = config.baseUrl;

const init = async () => {
    await fse.ensureDir(filesPath);
    await getResourceBundles();
    // await getOrgs()
    // await getChannel()
    // await frameworks()
    // await getForms()
};

const getOrgs = async () => {
    for (const id of config.organizations.ids) {
        const result = await axios.post(baseUrl + config.organizations.url,
            { request: { filters: { slug: id, isRootOrg: true } } });

        await fse.ensureFile(path.join(filesPath, config.organizations.dest_folder, id + ".json"));
        await fse.writeJson(path.join(filesPath, config.organizations.dest_folder, id + ".json"), result.data);
    }
};

const getChannel = async () => {
    for (const id of config.channels.ids) {
        const result = await axios.get(baseUrl + config.channels.url + id);
        await fse.ensureFile(path.join(filesPath, config.channels.dest_folder, id + ".json"));
        await fse.writeJson(path.join(filesPath, config.channels.dest_folder, id + ".json"), result.data);
    }
};

const getResourceBundles = async () => {
    for (const bundle of config.resourceBundles.files) {
        const result = await axios.get(baseUrl + config.resourceBundles.url + bundle);

        await fse.ensureFile(path.join(filesPath, config.resourceBundles.dest_folder, bundle + ".json"));
        await fse.writeJson(path.join(filesPath, config.resourceBundles.dest_folder, bundle + ".json"), result.data);
    }
};

const frameworks = async () => {
    for (const id of config.frameworks.ids) {
        const result = await axios.get(baseUrl + config.frameworks.url + id);
        await fse.ensureFile(path.join(filesPath, config.frameworks.dest_folder, id + ".json"));
        await fse.writeJson(path.join(filesPath, config.frameworks.dest_folder, id + ".json"), result.data);
    }
};

const getForms = async () => {
    for (const data of config.forms.requests_data) {
        const requestData = {
            request: data,
        };
        const result = await axios.post(baseUrl + config.forms.url,
            requestData);
        const filename = `${data.type}_${data.subType}_${data.action}_${data.rootOrgId}`;
        await fse.ensureFile(path.join(filesPath, config.forms.dest_folder, filename + ".json"));
        await fse.writeJson(path.join(filesPath, config.forms.dest_folder, filename + ".json"), result.data);
    }
};

init()
    .catch((err) => {
        // tslint:disable-next-line:no-console
        console.log("Error while preparing data", err);
    });
