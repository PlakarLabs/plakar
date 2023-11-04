import {dummmyFetchConfig, dummyFetchSnapshotPage, dummyFetchSnapshotsPath} from "./DemoRepo";

export function fetchConfig(apiUrl) {
    return dummmyFetchConfig();
}

export function fetchSnapshots(apiUrl, page, pageSize) {
    return dummyFetchSnapshotPage(apiUrl, page, pageSize);
}

export function fetchSnapshotsPath(apiUrl, pathId, page, pageSize) {
    return dummyFetchSnapshotsPath(apiUrl, pathId, page, pageSize);
}


export function search(searchParams) {
    return [];
}