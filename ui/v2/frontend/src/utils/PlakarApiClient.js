import {dummmyFetchConfig, dummyFetchSnapshotPage, dummyFetchSnapshotsPath, dummySearch} from "./DemoRepo";

export function fetchConfig(apiUrl) {
    return dummmyFetchConfig();
}

export function fetchSnapshots(apiUrl, page, pageSize) {
    return dummyFetchSnapshotPage(apiUrl, page, pageSize);
}

export function fetchSnapshotsPath(apiUrl, pathId, page, pageSize) {
    return dummyFetchSnapshotsPath(apiUrl, pathId, page, pageSize);
}


export function search(apiUrl, searchParams) {
    return dummySearch(apiUrl, searchParams);
}