export const SNAPSHOT_ROUTE = '/snapshot';
export const CONFIG_ROUTE = '/config';

export const snapshotURL = (snapshotId, path='/') => `${SNAPSHOT_ROUTE}/${snapshotId}:${path}`;