import {createDummySnapshotItems, fetchSnapshotPage} from "./DataGenerator";


const snapshots = createDummySnapshotItems(384);


function fetchSnapshots(apiUrl, page, pageSize) {
    return fetchSnapshotPage(snapshots, page, pageSize);
}

function fetchSnapshotsPath(apiUrl, pathId, page, pageSize) {
    return [];
}


function search(searchParams) {
    return [];
}