import {createDummySnapshotItems, fetchSnapshotPage} from "./DataGenerator";


export const snapshots = createDummySnapshotItems(384);


export function fetchSnapshots(apiUrl, page, pageSize) {
    return fetchSnapshotPage(snapshots, page, pageSize);
}

export function fetchSnapshotsPath(apiUrl, pathId, page, pageSize) {
    return {
        page: page,
        pageSize: pageSize,
        totalItems: 1,
        totalPages: 1,
        hasPreviousPage: false,
        hasNextPage: false,
        snapshot: {
            id: '1233545-1233545-1233545-1233545',
            uri: '/snapshot/1233545-1233545-1233545-1233545:/home/',
            shortId: '1233545',
            path: pathId.split(':')[1],
            isDirectory: true,
        },
        items: [
            {
                path: '/home',
                isDirectory: true,
                mode: 'drwxr-xr-x',
                uid: '1000',
                gid: '1000',
                date: '2021-10-10 12:00:00Z',
                size: '100 B',
            }
        ]
    }
}


function search(searchParams) {
    return [];
}