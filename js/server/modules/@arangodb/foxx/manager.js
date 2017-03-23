'use strict';

// //////////////////////////////////////////////////////////////////////////////
// / @brief Foxx service manager
// /
// / @file
// /
// / DISCLAIMER
// /
// / Copyright 2013 triagens GmbH, Cologne, Germany
// /
// / Licensed under the Apache License, Version 2.0 (the "License")
// / you may not use this file except in compliance with the License.
// / You may obtain a copy of the License at
// /
// /     http://www.apache.org/licenses/LICENSE-2.0
// /
// / Unless required by applicable law or agreed to in writing, software
// / distributed under the License is distributed on an "AS IS" BASIS,
// / WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// / See the License for the specific language governing permissions and
// / limitations under the License.
// /
// / Copyright holder is triAGENS GmbH, Cologne, Germany
// /
// / @author Dr. Frank Celler
// / @author Michael Hackstein
// / @author Copyright 2013, triAGENS GmbH, Cologne, Germany
// //////////////////////////////////////////////////////////////////////////////

const fs = require('fs');
const path = require('path');
const querystringify = require('querystring').encode;
const dd = require('dedent');
const utils = require('@arangodb/foxx/manager-utils');
const store = require('@arangodb/foxx/store');
const FoxxService = require('@arangodb/foxx/service');
const generator = require('@arangodb/foxx/generator');
const ensureServiceExecuted = require('@arangodb/foxx/routing').routeService;
const arangodb = require('@arangodb');
const ArangoError = arangodb.ArangoError;
const errors = arangodb.errors;
const aql = arangodb.aql;
const db = arangodb.db;
const ArangoClusterControl = require('@arangodb/cluster');
const request = require('@arangodb/request');
const actions = require('@arangodb/actions');

const systemServiceMountPoints = [
  '/_admin/aardvark', // Admin interface.
  '/_api/foxx', // Foxx management API.
  '/_api/gharial' // General_Graph API.
];

const GLOBAL_SERVICE_MAP = new Map();

const RANDOM_ID = require('@arangodb/crypto').genRandomAlphaNumbers(8);

function warn (e) {
  let err = e;
  while (err) {
    console.warnLines(
      err === e
      ? err.stack
      : `via ${err.stack}`
    );
    err = err.cause;
  }
}

function reloadRouting () {
  require('internal').executeGlobalContextFunction('reloadRouting');
  actions.reloadRouting();
}

function propagateServiceDestroyed (service) { // TODO
  reloadRouting();
  // clusterUninstallOnPeers(service)
}

// Installed, replaced or upgraded
function propagateServiceReplaced (service) { // TODO
  reloadRouting();
  // clusterReplaceOnPeers(service)
}

function propagateServiceReconfigured (service) { // TODO
  reloadRouting();
  // clusterReloadOnPeers(service)
}

function getServiceInstance (mount) { // done
  ensureFoxxInitialized();
  const localServiceMap = GLOBAL_SERVICE_MAP.get(db._name());
  if (localServiceMap.has(mount)) {
    return localServiceMap.get(mount);
  }
  return reloadInstalledService(mount);
}

function clusterReplaceOnPeers (service) { // TODO
  const bundleBuf = fs.readBuffer(service.bundlePath);
  let boundary = '--------------------------';
  for (let i = 0; i < 24; i++) {
    boundary += Math.floor(Math.random() * 10).toString(16);
  }
  const CRLF = '\r\n';
  const body = Buffer.concat([
    new Buffer(
      `--${boundary}${CRLF
      }content-disposition: form-data; name="service"; filename="tmp.zip"${CRLF
      }content-type: application/octet-stream${CRLF
      }content-length: ${bundleBuf.length}${CRLF
      }content-transfer-encoding: binary${CRLF
      }${CRLF}`
    ),
    bundleBuf,
    new Buffer(`${CRLF}--${boundary}--`)
  ]);
  const headers = {
    'content-type': `multipart/form-data; boundary=${boundary}`,
    'content-length': body.length
  };
  const url = `/_api/foxx/_clusterdist?${querystringify({
    chksum: service.checksum,
    mount: service.mount
  })}`;
  const myCoordinatorId = global.ArangoServerState.id();
  const options = {coordTransactionID: global.ArangoClusterComm.getId()};
  let pending = 0;
  for (const coordinator of global.ArangoClusterInfo.getCoordinators()) {
    if (coordinator === myCoordinatorId) {
      continue;
    }
    options.clientTransactionID = global.ArangoClusterInfo.uniqid();
    global.ArangoClusterComm.asyncRequest(
      'PUT',
      `server:${coordinator}`,
      db._name(),
      url,
      body,
      headers,
      options
    );
    pending++;
  }
  delete options.clientTransactionID;
  ArangoClusterControl.wait(options, pending);
}

function clusterUninstallOnPeers (service) { // TODO
  const url = `/_api/foxx/_clusterdist?${querystringify({
    mount: service.mount
  })}`;
  const myCoordinatorId = global.ArangoServerState.id();
  const options = {coordTransactionID: global.ArangoClusterComm.getId()};
  let pending = 0;
  for (const coordinator of global.ArangoClusterInfo.getCoordinators()) {
    if (coordinator === myCoordinatorId) {
      continue;
    }
    options.clientTransactionID = global.ArangoClusterInfo.uniqid();
    global.ArangoClusterComm.asyncRequest(
      'DELETE',
      `server:${coordinator}`,
      db._name(),
      url,
      null,
      {},
      options
    );
    pending++;
  }
  delete options.clientTransactionID;
  ArangoClusterControl.wait(options, pending);
}

function clusterReloadOnPeers () { // TODO
}

function refreshServiceBundlesFromFoxxmaster (...mountPoints) { // TODO
}

function initLocalServiceMap () { // done
  console.log('INITIALISING FOXX IN CTX', RANDOM_ID, db._name())
  console.trace()
  const localServiceMap = new Map();

  for (const serviceDefinition of utils.getStorage().all()) {
    const service = FoxxService.create(serviceDefinition);
    localServiceMap.set(service.mount, service);
  }

  for (const mount of systemServiceMountPoints) {
    localServiceMap.set(mount, installSystemServiceFromDisk(mount));
  }

  GLOBAL_SERVICE_MAP.set(db._name(), localServiceMap);
}

function reloadInstalledService (mount, runSetup) { // done
  const serviceDefinition = utils.getServiceDefinition(mount);
  console.log('Reloading', serviceDefinition.mount)
  if (!serviceDefinition) {
    throw new ArangoError({
      errorNum: errors.ERROR_SERVICE_NOT_FOUND.code,
      errorMessage: dd`
        ${errors.ERROR_SERVICE_NOT_FOUND.message}
        Mount path: "${mount}".
      `
    });
  }
  const service = FoxxService.create(serviceDefinition);
  if (runSetup) {
    service.executeScript('setup');
  }
  GLOBAL_SERVICE_MAP.get(db._name()).set(mount, service);
  return service;
}

function installedServices () {
  ensureFoxxInitialized();
  return Array.from(GLOBAL_SERVICE_MAP.get(db._name()).values());
}

function installSystemServiceFromDisk (mount) { // done
  const options = utils.getServiceDefinition(mount);
  const service = FoxxService.create(Object.assign({mount}, options));
  const serviceDefinition = service.toJSON();
  db._query(aql`
    UPSERT {mount: ${mount}}
    INSERT ${serviceDefinition}
    REPLACE ${serviceDefinition}
    IN ${utils.getStorage()}
  `);
  service.executeScript('setup');
  return service;
}

function ensureServiceLoaded (mount) { // done
  const service = getServiceInstance(mount);
  return ensureServiceExecuted(service, false);
}

function extractServiceToPath (archive, targetPath, isTemporaryFile) { // WTF?
  try {
    const tempFolder = fs.getTempFile('zip', false);
    fs.makeDirectory(tempFolder);
    fs.unzipFile(archive, tempFolder, false, true);

    let found;
    for (const filename of fs.listTree(tempFolder).sort((a, b) => a.length - b.length)) {
      if (filename === 'manifest.json' || filename.endsWith('/manifest.json')) {
        found = filename;
        break;
      }
    }

    if (!found) {
      throw new ArangoError({
        errorNum: errors.ERROR_SERVICE_MANIFEST_NOT_FOUND.code,
        errorMessage: dd`
          ${errors.ERROR_SERVICE_MANIFEST_NOT_FOUND.message}
          Source: ${tempFolder}
        `
      });
    }

    var basePath = path.dirname(path.resolve(tempFolder, found));
    fs.move(basePath, targetPath);

    if (found !== 'manifest.json') { // WTF?
      try {
        fs.removeDirectoryRecursive(tempFolder);
      } catch (e) {
        warn(Object.assign(
          new Error(`Cannot remove temporary folder "${tempFolder}"`),
          {cause: e}
        ));
      }
    }
  } finally {
    if (isTemporaryFile) {
      try {
        fs.remove(archive);
      } catch (e) {
        warn(Object.assign(
          new Error(`Cannot remove temporary file "${archive}"`),
          {cause: e}
        ));
      }
    }
  }
}

function downloadServiceBundleFromRemote (url) { // done
  try {
    const res = request.get(url, {encoding: null});
    res.throw();
    const tempFile = fs.getTempFile('downloads', false);
    fs.writeFileSync(tempFile, res.body);
    return tempFile;
  } catch (e) {
    throw Object.assign(
      new ArangoError({
        errorNum: errors.ERROR_SERVICE_SOURCE_ERROR.code,
        errorMessage: dd`
          ${errors.ERROR_SERVICE_SOURCE_ERROR.message}
          URL: ${url}
        `
      }),
      {cause: e}
    );
  }
}

function patchManifestFile (servicePath, patchData) { // done
  const filename = path.join(servicePath, 'manifest.json');
  let manifest;
  try {
    const rawManifest = fs.readFileSync(filename, 'utf-8');
    manifest = JSON.parse(rawManifest);
  } catch (e) {
    throw Object.assign(
      new ArangoError({
        errorNum: errors.ERROR_MALFORMED_MANIFEST_FILE.code,
        errorMessage: dd`
          ${errors.ERROR_MALFORMED_MANIFEST_FILE.message}
          File: ${filename}
        `
      }), {cause: e}
    );
  }
  Object.assign(manifest, patchData);
  fs.writeFileSync(filename, JSON.stringify(manifest, null, 2));
}

function _buildServiceInPath (serviceInfo, destPath, options = {}) { // okay-ish
  if (serviceInfo === 'EMPTY') {
    const generated = generator.generate(options);
    generator.write(destPath, generated.files, generated.folders);
  } else {
    if (/^GIT:/i.test(serviceInfo)) {
      const splitted = serviceInfo.split(':');
      const baseUrl = process.env.FOXX_BASE_URL || 'https://github.com';
      serviceInfo = `${baseUrl}${splitted[1]}/archive/${splitted[2] || 'master'}.zip`;
    } else if (/^uploads[/\\]tmp-/.test(serviceInfo)) {
      serviceInfo = path.join(fs.getTempPath(), serviceInfo);
    }
    if (/^https?:/i.test(serviceInfo)) {
      const tempFile = downloadServiceBundleFromRemote(serviceInfo);
      extractServiceToPath(tempFile, destPath, true);
    } else if (utils.pathRegex.test(serviceInfo)) {
      if (fs.isDirectory(serviceInfo)) {
        const tempFile = utils.zipDirectory(serviceInfo);
        extractServiceToPath(tempFile, destPath, true);
      } else if (!fs.exists(serviceInfo)) {
        throw new ArangoError({
          errorNum: errors.ERROR_SERVICE_SOURCE_NOT_FOUND.code,
          errorMessage: dd`
            ${errors.ERROR_SERVICE_SOURCE_NOT_FOUND.message}
            Path: ${serviceInfo}
          `
        });
      } else {
        extractServiceToPath(serviceInfo, destPath, false);
      }
    } else {
      if (options.refresh) {
        try {
          store.update();
        } catch (e) {
          warn(e);
        }
      }
      const info = store.installationInfo(serviceInfo);
      const tempFile = downloadServiceBundleFromRemote(info.url);
      extractServiceToPath(tempFile, destPath, true);
      patchManifestFile(destPath, info.manifest);
    }
  }
  if (options.legacy) {
    patchManifestFile(destPath, {engines: {arangodb: '^2.8.0'}});
  }
}

function _validateService (serviceInfo, mount) { // WTF?
  const tempPath = fs.getTempFile('apps', false);
  try {
    _buildServiceInPath(serviceInfo, tempPath, {});
    const tmp = FoxxService.create({mount, basePath: tempPath});
    if (!tmp.needsConfiguration()) {
      ensureServiceExecuted(tmp, true);
    }
  } finally {
    fs.removeDirectoryRecursive(tempPath, true);
  }
}

function _install (serviceInfo, mount, options = {}) { // WTF?
  const servicePath = FoxxService.basePath(mount);
  if (fs.exists(servicePath)) {
    throw new ArangoError({
      errorNum: errors.ERROR_SERVICE_MOUNTPOINT_CONFLICT.code,
      errorMessage: dd`
        ${errors.ERROR_SERVICE_MOUNTPOINT_CONFLICT.message}
        Mount path: "${mount}".
      `
    });
  }
  fs.makeDirectoryRecursive(path.dirname(servicePath));
  // Remove the empty APP folder.
  // Otherwise move will fail.
  if (fs.exists(servicePath)) {
    fs.removeDirectory(servicePath);
  }

  ensureFoxxInitialized();

  try {
    _buildServiceInPath(serviceInfo, servicePath, options);
  } catch (e) {
    try {
      fs.removeDirectoryRecursive(servicePath, true);
    } catch (err) {}
    throw e;
  }

  createServiceBundle(mount);

  try {
    const service = FoxxService.create({mount, options, noisy: true});
    service.updateChecksum();
    const serviceDefinition = service.toJSON();
    db._query(aql`
      UPSERT {mount: ${mount}}
      INSERT ${serviceDefinition}
      REPLACE ${serviceDefinition}
      IN ${utils.getStorage()}
    `);
    GLOBAL_SERVICE_MAP.get(db._name()).set(mount, service);
    if (options.setup !== false) {
      service.executeScript('setup');
    }
    ensureServiceExecuted(service, true);
    return service;
  } catch (e) {
    try {
      fs.removeDirectoryRecursive(servicePath, true);
    } catch (e) {
      warn(e);
    }
    const collection = utils.getStorage();
    db._query(aql`
      FOR service IN ${collection}
      FILTER service.mount == ${mount}
      REMOVE service IN ${collection}
    `);
    throw e;
  }
}

function _uninstall (mount, options = {}) { // WTF?
  let service;
  try {
    service = getServiceInstance(mount);
  } catch (e) {
    if (!options.force) {
      throw e;
    }
    warn(e);
  }
  const collection = utils.getStorage();
  db._query(aql`
    FOR service IN ${collection}
    FILTER service.mount == ${mount}
    REMOVE service IN ${collection}
  `);
  GLOBAL_SERVICE_MAP.get(db._name()).delete(mount);
  if (service && options.teardown !== false) {
    try {
      service.executeScript('teardown');
    } catch (e) {
      if (!options.force) {
        throw e;
      }
      warn(e);
    }
  }
  try {
    const servicePath = FoxxService.basePath(mount);
    fs.removeDirectoryRecursive(servicePath, true);
  } catch (e) {
    if (!options.force) {
      throw e;
    }
    warn(e);
  }
  return service;
}

function startup (fixMissingChecksums) {
  const db = require('internal').db;
  const dbName = db._name();
  try {
    db._useDatabase('_system');
    const databases = db._databases();
    for (const name of databases) {
      try {
        db._useDatabase(name);
        rebuildAllServiceBundles(fixMissingChecksums);
      } catch (e) {
        let err = e;
        while (err) {
          console.warnLines(
            err === e
            ? err.stack
            : `via ${err.stack}`
          );
          err = err.cause;
        }
      }
    }
  } finally {
    // return to _system database so the caller does not need to know we changed the db
    db._useDatabase(dbName);
  }
}

function rebuildAllServiceBundles (updateDatabase, fixMissingChecksums) {
  const servicesMissingChecksums = [];
  const collection = utils.getStorage();
  for (const serviceDefinition of collection.all()) {
    const mount = serviceDefinition.mount;
    if (mount.startsWith('/_')) {
      continue;
    }
    createServiceBundle(mount);
    if (fixMissingChecksums && !serviceDefinition.checksum) {
      servicesMissingChecksums.push({
        checksum: FoxxService.checksum(mount),
        _key: serviceDefinition._key
      });
    }
  }
  if (!servicesMissingChecksums.length) {
    return;
  }
  db._query(aql`
    FOR service IN ${servicesMissingChecksums}
    UPDATE service._key
    WITH {checksum: service.checksum}
    IN ${collection}
  `);
}

function selfHeal () {
  const db = require('internal').db;
  const dbName = db._name();
  try {
    db._useDatabase('_system');
    const databases = db._databases();
    for (const name of databases) {
      try {
        db._useDatabase(name);
        healMyselfAndCoords();
      } catch (e) {
        let err = e;
        while (err) {
          console.warnLines(
            err === e
            ? err.stack
            : `via ${err.stack}`
          );
          err = err.cause;
        }
      }
    }
  } finally {
    // return to _system database so the caller does not need to know we changed the db
    db._useDatabase(dbName);
  }
  reloadRouting() // :(
}

function healMyselfAndCoords () {
  // checksumsINeedToFixLocally = List<mount>
  // actualChecksums = Map<mount, checksum>
  // coordsKnownToBeGoodSources = Map<mount, List<id>>
  // coordsKnownToBeBadSources = Map<mount, Map<id, checksum>>
  // allKnownMounts = List<mount>

  // FOR {mount, checksum} IN _apps:
  //   FILTER !mount.startsWith('/_')
  //   allKnownMounts.push(mount)
  //   actualChecksums.set(mount, checksum)
  //   coordsKnownToBeGoodSources.set(mount, [])
  //   coordsKnownToBeBadSources.set(mount, Map())
  //   IF !checksum || checksum != FoxxService.checksum(mount):
  //     checksumsINeedToFixLocally.push(mount)

  // FOR id IN coordinatorIds:
  //   FILTER id != myId
  //   coordChecksums := Map<mount, checksum>
  //   coordChecksums = Coordinator(id)->getChecksums(allKnownMounts)
  //   FOR [mount, checksum] IN coordChecksums.items():
  //     IF !checksum:
  //       coordsKnownToBeBadSources.get(mount).set(id, null)
  //     ELIF !actualChecksums.get(mount):
  //       actualChecksums.set(mount, checksum)
  //       coordsKnownToBeGoodSources.get(mount).push(id)
  //     ELIF actualChecksums.get(mount) == checksum:
  //       coordsKnownToBeGoodSources.get(mount).push(id)
  //     ELSE:
  //       coordsKnownToBeBadSources.get(mount).set(id, checksum)
  // DEL actualChecksums

  // mountsINeedToDeleteInCollection = List<mount>
  // checksumsINeedToFixInCollection = Map<mount, checksum>
  // FOR mount IN checksumsINeedToFixLocally:
  //   possibleSources = coordsKnownToBeGoodSources.get(mount)
  //   IF !possibleSources.length:
  //     arbitraryChecksum = FoxxService.checksum(mount)
  //     IF arbitraryChecksum:
  //       checksumsINeedToFixInCollection.set(mount, arbitraryChecksum)
  //       possibleSources.push(myId)
  //     ELSE:
  //       found = false
  //       FOR [coordId, coordChecksum] IN coordsKnownToBeBadSources.get(mount).items():
  //         FILTER coordChecksum
  //         checksumsINeedToFixInCollection.set(mount, coordChecksum)
  //         possibleSources.push(coordId)
  //         bundle = Coordinator(id)->getBundle(mount, checksum)
  //         replaceLocalBundle(mount, bundle)
  //         extractBundle(bundle, mount)
  //         found = true
  //         BREAK
  //       IF !found:
  //         mountsINeedToDeleteInCollection.push(mount)
  //         coordsKnownToBeBadSources.remove(mount)
  //   ELSE:
  //     id = possibleSources[0]
  //     bundle = Coordinator(id)->getBundle(mount, checksum)
  //     replaceLocalBundle(mount, bundle)
  //     extractBundle(bundle, mount)

  // FOR [mount, ids] IN coordsKnownToBeGoodSources.items():
  //   ids.push(myId)

  // FOR mount IN mountsINeedToDeleteInCollection:
  // REMOVE mount IN _apps

  // FOR {mount, checksum} IN checksumsINeedToFixInCollection
  // UPDATE mount WITH {checksum} IN _apps

  // FOR id IN coordinatorIds:
  //   FILTER id != myId
  //   servicesYouNeedToUpdate = Map<mount, id>
  //   FOR [mount, map] IN coordsKnownToBeBadSources.items():
  //     FILTER map.has(id)
  //     goodId = RANDOM_CHOICE(coordsKnownToBeGoodSources.get(mount))
  //     servicesYouNeedToUpdate.set(mount, goodId)
  //   Coordinator(id)->goUpdateYourself(servicesYouNeedToUpdate)
}

function createServiceBundle (mount) {
  const servicePath = FoxxService.basePath(mount);
  const bundlePath = FoxxService.bundlePath(mount);
  if (fs.exists(bundlePath)) {
    fs.remove(bundlePath);
  }
  fs.makeDirectoryRecursive(path.dirname(bundlePath));
  utils.zipDirectory(servicePath, bundlePath);
}

// CRUD

function install (serviceInfo, mount, options = {}) { // done
  utils.validateMount(mount);
  ensureFoxxInitialized();
  const service = _install(serviceInfo, mount, options);
  propagateServiceReplaced(service);
  return service;
}

function uninstall (mount, options = {}) { // done
  ensureFoxxInitialized();
  const service = _uninstall(mount, options);
  propagateServiceDestroyed(service);
  return service;
}

function replace (serviceInfo, mount, options = {}) { // done
  utils.validateMount(mount);
  ensureFoxxInitialized();
  _validateService(serviceInfo, mount);
  _uninstall(mount, Object.assign({teardown: true}, options, {force: true}));
  const service = _install(serviceInfo, mount, Object.assign({}, options, {force: true}));
  propagateServiceReplaced(service);
  return service;
}

function upgrade (serviceInfo, mount, options = {}) { // done
  ensureFoxxInitialized();
  _validateService(serviceInfo, mount);
  const oldService = getServiceInstance(mount);
  const serviceOptions = oldService.toJSON().options;
  Object.assign(serviceOptions.configuration, options.configuration);
  Object.assign(serviceOptions.dependencies, options.dependencies);
  serviceOptions.development = options.development;
  _uninstall(mount, Object.assign({teardown: false}, options, {force: true}));
  const service = _install(serviceInfo, mount, Object.assign({}, options, serviceOptions, {force: true}));
  propagateServiceReplaced(service);
  return service;
}

// -------------------------------------------------
// Functions for manipulating services
// -------------------------------------------------

function runScript (scriptName, mount, options) { // done
  let service = getServiceInstance(mount);
  if (service.isDevelopment) {
    const runSetup = scriptName !== 'setup';
    service = reloadInstalledService(mount, runSetup);
  }
  ensureServiceLoaded(mount);
  const result = service.executeScript(scriptName, options);
  return result === undefined ? null : result;
}

function runTests (mount, options = {}) { // done
  let service = getServiceInstance(mount);
  if (service.isDevelopment) {
    service = reloadInstalledService(mount, true);
  }
  ensureServiceLoaded(mount);
  return require('@arangodb/foxx/mocha').run(service, options.reporter);
}

function setDevelopmentMode (mount, enabled) { // done
  const service = getServiceInstance(mount);
  service.development(enabled);
  utils.updateService(mount, service.toJSON());
  if (enabled) {
    propagateServiceReconfigured(service);
  } else {
    // Make sure setup changes from devmode are respected
    service.executeScript('setup');
    propagateServiceReplaced(service);
  }
  return service;
}

function setConfiguration (mount, options = {}) { // done
  const service = getServiceInstance(mount);
  const warnings = service.applyConfiguration(options.configuration, options.replace);
  utils.updateService(mount, service.toJSON());
  propagateServiceReconfigured(service);
  return warnings;
}

function setDependencies (mount, options = {}) { // done
  const service = getServiceInstance(mount);
  const warnings = service.applyDependencies(options.dependencies, options.replace);
  utils.updateService(mount, service.toJSON());
  propagateServiceReconfigured(service);
  return warnings;
}

// -------------------------------------------------
// Misc functions
// -------------------------------------------------

function requireService (mount) { // okay-ish
  mount = '/' + mount.replace(/(^\/+|\/+$)/, '');
  const service = getServiceInstance(mount);
  return ensureServiceExecuted(service, true).exports;
}

function ensureFoxxInitialized () { // done
  if (!GLOBAL_SERVICE_MAP.has(db._name())) {
    initLocalServiceMap();
  }
}

function getMountPoints () { // WTF?
  ensureFoxxInitialized();
  return Array.from(GLOBAL_SERVICE_MAP.get(db._name()).keys());
}

function listJson () { // done
  ensureFoxxInitialized();
  const json = [];
  for (const service of GLOBAL_SERVICE_MAP.get(db._name()).values()) {
    json.push({
      mount: service.mount,
      name: service.manifest.name,
      description: service.manifest.description,
      author: service.manifest.author,
      system: service.isSystem,
      development: service.isDevelopment,
      contributors: service.manifest.contributors || false,
      license: service.manifest.license,
      version: service.manifest.version,
      path: service.basePath,
      config: service.getConfiguration(),
      deps: service.getDependencies(),
      scripts: service.getScripts()
    });
  }
  return json;
}

// -------------------------------------------------
// Exports
// -------------------------------------------------

exports.install = install;
exports.uninstall = uninstall;
exports.replace = replace;
exports.upgrade = upgrade;
exports.runTests = runTests;
exports.runScript = runScript;
exports.development = (mount) => setDevelopmentMode(mount, true);
exports.production = (mount) => setDevelopmentMode(mount, false);
exports.setConfiguration = setConfiguration;
exports.setDependencies = setDependencies;
exports.requireService = requireService;
exports.lookupService = getServiceInstance;
exports.installedServices = installedServices;

// -------------------------------------------------
// Exported internals
// -------------------------------------------------

exports.reloadInstalledService = reloadInstalledService;
exports.ensureRouted = ensureServiceLoaded;
exports.initializeFoxx = initLocalServiceMap;
exports.ensureFoxxInitialized = ensureFoxxInitialized;
exports._startup = startup;
exports._selfHeal = selfHeal;
exports._resetCache = () => GLOBAL_SERVICE_MAP.clear();
exports._mountPoints = getMountPoints;
exports.listJson = listJson;

// -------------------------------------------------
// Exports from foxx utils module
// -------------------------------------------------

exports.getServiceDefinition = utils.getServiceDefinition;
exports.list = utils.list;
exports.listDevelopment = utils.listDevelopment;
exports.listDevelopmentJson = utils.listDevelopmentJson;

// -------------------------------------------------
// Exports from foxx store module
// -------------------------------------------------

exports.available = store.available;
exports.availableJson = store.availableJson;
exports.getFishbowlStorage = store.getFishbowlStorage;
exports.search = store.search;
exports.searchJson = store.searchJson;
exports.update = store.update;
exports.info = store.info;
