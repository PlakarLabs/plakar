export const SNAPSHOT_ROUTE = '/snapshot';
export const CONFIG_ROUTE = '/config';

export const topDirectoryURL = (snapshotId, path='/') => `${SNAPSHOT_ROUTE}/${snapshotId}:${path}`;

export const snapshotListPageURL= (page=1, pageSize=10) => `${SNAPSHOT_ROUTE}?page=${page}&pageSize=${pageSize}`;

export const snapshotURL = (snapshotId, path='/', page=1, pageSize=10) => `${SNAPSHOT_ROUTE}/${snapshotId}:${path}?page=${page}&pageSize=${pageSize}`;


export const directoryURL = (path, page=1, pageSize=10) => `${SNAPSHOT_ROUTE}/${path}?page=${page}&pageSize=${pageSize}`;
