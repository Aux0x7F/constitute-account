// FILE: identity/sw/swarm/index.js
export {
  makeIdentityRecord,
  makeDeviceRecord,
  makeDhtRecord,
  putIdentityRecord,
  putDeviceRecord,
  putDhtRecord,
  getIdentityRecord,
  getDeviceRecord,
  getDhtRecord,
  listIdentityRecords,
  listDeviceRecords,
  listDhtRecords,
  validateRecord,
} from './discovery.js';

export {
  resolveIdentityById,
  resolveDeviceByPk,
  resolveIdentityForDevice,
} from './resolve.js';
