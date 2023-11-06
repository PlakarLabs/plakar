export const SNAPSHOT_ROUTE = '/snapshot';
export const CONFIG_ROUTE = '/config';

export const snapshotListPageURL= (page=1, pageSize=10) => `${SNAPSHOT_ROUTE}?page=${page}&pageSize=${pageSize}`;

export const snapshotURL = (snapshotId, path='/', page=1, pageSize=10) => `${SNAPSHOT_ROUTE}/${snapshotId}:${path}?page=${page}&pageSize=${pageSize}`;