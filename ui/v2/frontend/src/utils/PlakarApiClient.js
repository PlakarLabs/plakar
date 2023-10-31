import {createDummySnapshotItems, fetchSnapshotPage} from "./DataGenerator";


export const snapshots = createDummySnapshotItems(384);


export function fetchSnapshots(apiUrl, page, pageSize) {
    return fetchSnapshotPage(snapshots, page, pageSize);
}

function fetchSnapshotsPath(apiUrl, pathId, page, pageSize) {
    return [];
}


function search(searchParams) {
    return [];
}