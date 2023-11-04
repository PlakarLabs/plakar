import {createDummySnapshotItems, fetchSnapshotPage} from "./DataGenerator";
import {faker} from '@faker-js/faker';

export const snapshots = createDummySnapshotItems(384);
export const snapshotIndex = snapshots.reduce(
    (acc, snapshot) => {
    acc[snapshot.id] = snapshot;
    return acc;
}, {});

export function dummyFetchSnapshotPage(apiUrl, page, pageSize) {
    return fetchSnapshotPage(snapshots, page, pageSize);
}

export function demoJpegFile(apiUrl, pathId, page, pageSize) {
    return {
        name: 'demo.jpeg',
        path: '/demo.jpeg',
        mimeType: 'image/jpeg',
        size: '3,4 Mb',
        modificationDate: '2021-10-10 12:00:00Z',
        Checksum: faker.git.commitSha({length: 40}),
        mode: '-rwxr-xr-x',
        uid: '1000',
        gid: '1000',
        device: '123333',
        inode: '123333',
    }
}


export function demoJSFile(apiUrl, pathId, page, pageSize) {
    return {
        name: 'demo.js',
        directoryPath: `${pathId}`,
        path: `${pathId}`,
        mimeType: 'text/javascript',
        size: '3,4 Mb',
        modificationDate: '2021-10-10 12:00:00Z',
        Checksum: faker.git.commitSha({length: 40}),
        mode: '-rwxr-xr-x',
        uid: '1000',
        gid: '1000',
        device: '123333',
        inode: '123333',
    }
}

export function dummyFetchSnapshotsPath(apiUrl, pathId, page, pageSize) {
    const snapshotId = pathId.split(':')[0];
    console.log('snapshotId', snapshotId)
    const s = snapshotIndex[snapshotId];
    console.log('snapshot found ?', s)
    console.log('index', snapshotIndex)

    let baseResponse = {
        page: page,
        pageSize: pageSize,
        totalItems: 1,
        totalPages: 1,
        hasPreviousPage: false,
        hasNextPage: false,
        snapshot: s,
        items: [],
    };


    if (pathId.endsWith('demo.jpeg')) {
        baseResponse.items.push(demoJpegFile(apiUrl, pathId, page, pageSize));
    }
    if (pathId.endsWith('demo.js')) {
        baseResponse.items.push(demoJSFile(apiUrl, pathId, page, pageSize));
    } else {
        baseResponse.items = [{
            name: 'home',
            path: `${pathId}home/`,
            isDirectory: true,
            mode: 'drwxr-xr-x',
            uid: '1000',
            gid: '1000',
            date: '2021-10-10 12:00:00Z',
            size: '100 B',
        }, {
            name: 'super-folder',
            path: `${pathId}/super-folder/`,
            isDirectory: true,
            mode: 'drwxr-xr-x',
            uid: '1000',
            gid: '1000',
            date: '2021-10-10 12:00:00Z',
            size: '100 B',
        }, {
            name: 'demo.js',
            path: `${pathId}demo.js`,
            isDirectory: false,
            mode: 'drw-r--r--',
            uid: '1000',
            gid: '1000',
            date: '2021-10-10 12:00:00Z',
            size: '100 B',
        }, {
            name: 'demo.mp4',
            path: '/home/demo.mp4',
            isDirectory: false,
            mode: 'drw-r--r--',
            uid: '1000',
            gid: '1000',
            date: '2021-10-10 12:00:00Z',
            size: '100 B',
        }, {
            name: 'demo.mp3',
            path: '/home/demo.mp3',
            isDirectory: false,
            mode: 'drw-r--r--',
            uid: '1000',
            gid: '1000',
            date: '2021-10-10 12:00:00Z',
            size: '100 B',
        }, {
            name: 'demo-small.jpeg',
            path: '/home/demo-small.jpeg',
            isDirectory: false,
            mode: 'drw-r--r--',
            uid: '1000',
            gid: '1000',
            date: '2021-10-10 12:00:00Z',
            size: '100 B',
        }, {
            name: 'demo-fat.jpeg',
            path: '/home/demo-fat.jpeg',
            isDirectory: false,
            mode: 'drw-r--r--',
            uid: '1000',
            gid: '1000',
            date: '2021-10-10 12:00:00Z',
            size: '100 B',
        },]
    }
    // return a promise
    return new Promise((resolve, reject) => {
        // Simulating a server request with a timeout
        setTimeout(() => {
            // Let's say the operation was successful
            resolve(baseResponse);
            // If something goes wrong, you would use reject(new Error('Error message'));
        }, 1000);
    });
}