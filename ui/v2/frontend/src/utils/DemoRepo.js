//import {createDummySnapshotItems, fetchSnapshotPage} from "./DataGenerator";
//import {faker} from '@faker-js/faker';
//import {getDirectoryPath} from "./Path";

/*
export const dummmyFetchConfig = () => {
    return new Promise((resolve, reject) => {
        // Simulating a server request with a timeout
        setTimeout(() => {
            // Let's say the operation was successful
            resolve({
                repository: 'poolp.org'
            });
            // If something goes wrong, you would use reject(new Error('Error message'));
        }, 1000);
    });
}
*/

/*
export function createOrRestoreSnapshots(size) {
    let items = localStorage.getItem('snapshots')
    if (!items) {
        console.log('creating new snapshots')
        items = JSON.stringify(createDummySnapshotItems(size))
        localStorage.setItem('snapshots', items);
    }
    console.log('restoring snapshots')
    return JSON.parse(items);
}
//const snapshots = createOrRestoreSnapshots(54);

export async function dummyFetchSnapshotPage(apiUrl, page, pageSize) {
    var snapshots = createOrRestoreSnapshots(54);
    return fetchSnapshotPage(snapshots, page, pageSize);
}
*/

/*
export function demoJpegSmallFile(apiUrl, pathId, page, pageSize) {
    return {
        name: 'demo-small.jpg',
        directoryPath: `${pathId}`,
        path: `${pathId}`,
        rawPath: `http://localhost:3000/demo-files/demo.jpeg`,
        mimeType: 'image/jpeg',
        size: '95 Kb',
        byteSize: 97271,
        modificationDate: '2021-10-10 12:00:00Z',
        checksum: faker.git.commitSha({length: 40}),
        mode: '-rwxr-xr-x',
        uid: '1000',
        gid: '1000',
        device: '123333',
        inode: '123333',
    }
}
*/


/*
export function demoJpegFatFile(apiUrl, pathId, page, pageSize) {
    return {
        name: 'demo-small.jpg',
        directoryPath: `${getDirectoryPath(pathId)}/`,
        path: `${pathId}`,
        rawPath: `http://localhost:3000/demo-files/demo.jpeg`,
        mimeType: 'image/jpeg',
        size: '300 Mb',
        byteSize: 314572800,
        modificationDate: '2021-10-10 12:00:00Z',
        checksum: faker.git.commitSha({length: 40}),
        mode: '-rwxr-xr-x',
        uid: '1000',
        gid: '1000',
        device: '123333',
        inode: '123333',
    }
}
*/

/*
export function demoMp4File(apiUrl, pathId, page, pageSize) {
    return {
        name: 'demo.mp4',
        directoryPath: `${getDirectoryPath(pathId)}/`,
        path: `${pathId}`,
        rawPath: `http://localhost:3000/demo-files/demo.mp4`,
        mimeType: 'video/mp4',
        size: '590 KB',
        byteSize: 604534,
        modificationDate: '2021-10-10 12:00:00Z',
        checksum: faker.git.commitSha({length: 40}),
        mode: '-rwxr-xr-x',
        uid: '1000',
        gid: '1000',
        device: '123333',
        inode: '123333',
    }
}
*/

/*
export function demoAudioFile(apiUrl, pathId, page, pageSize) {
    return {
        name: 'demo.mp3',
        directoryPath: `${getDirectoryPath(pathId)}/`,
        path: `${pathId}`,
        rawPath: `http://localhost:3000/demo-files/demo.mp3`,
        mimeType: 'audio/mp3',
        size: '144 KB',
        byteSize: 147961,
        modificationDate: '2021-10-10 12:00:00Z',
        checksum: faker.git.commitSha({length: 40}),
        mode: '-rwxr-xr-x',
        uid: '1000',
        gid: '1000',
        device: '123333',
        inode: '123333',
    }
}
*/

/*
export function demoJSFile(apiUrl, pathId, page, pageSize) {
    return {
        name: 'demo.js',
        directoryPath: `${getDirectoryPath(pathId)}/`,
        path: `${pathId}`,
        rawPath: `http://localhost:3000/demo-files/demo.js`,
        mimeType: 'text/javascript',
        size: '433 B',
        byteSize: 433,
        modificationDate: '2021-10-10 12:00:00Z',
        checksum: faker.git.commitSha({length: 40}),
        mode: '-rwxr-xr-x',
        uid: '1000',
        gid: '1000',
        device: '123333',
        inode: '123333',
    }
}
*/

/*
export function demoTextFile(apiUrl, pathId, page, pageSize) {
    return {
        name: 'demo.js',
        directoryPath: `${getDirectoryPath(pathId)}/`,
        path: `${pathId}`,
        rawPath: `http://localhost:3000/demo-files/demo.txt`,
        mimeType: 'text/plain',
        size: '433 B',
        byteSize: 433,
        modificationDate: '2021-10-10 12:00:00Z',
        checksum: faker.git.commitSha({length: 40}),
        mode: '-rwxr-xr-x',
        uid: '1000',
        gid: '1000',
        device: '123333',
        inode: '123333',
    }
}
*/

/*
export const dummyFetchSnapshotsPath = async (apiUrl, pathId, page, pageSize) => {
    const snapshotId = pathId.split(':')[0];
    const path = pathId.split(':')[1];
    // wait for 1 second
    var snapshots = createOrRestoreSnapshots(54);

    const r = snapshots.filter((elem) => elem.id === snapshotId);
    const s = r.length > 0 ? r[0] : null;

    let baseResponse = {
        page: page,
        pageSize: pageSize,
        totalItems: 1,
        totalPages: 10,
        hasPreviousPage: false,
        hasNextPage: false,
        snapshot: s,
        path: path,
        items: [],
    };

    return baseResponse;

    if (pathId.endsWith('demo-small.jpg')) {
        baseResponse.items = [demoJpegSmallFile(apiUrl, pathId, page, pageSize)];
    } else if (pathId.endsWith('demo-fat.jpg')) {
        baseResponse.items = [demoJpegFatFile(apiUrl, pathId, page, pageSize)];
    } else if (pathId.endsWith('demo.mp4')) {
        baseResponse.items = [demoMp4File(apiUrl, pathId, page, pageSize)];
    } else if (pathId.endsWith('demo.mp3')) {
        baseResponse.items = [demoAudioFile(apiUrl, pathId, page, pageSize)];
    } else if (pathId.endsWith('demo.js')) {
        baseResponse.items = [demoJSFile(apiUrl, pathId, page, pageSize)];
    } else if (pathId.endsWith('demo.txt')) {
        baseResponse.items = [demoTextFile(apiUrl, pathId, page, pageSize)];
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
            path: `${pathId}super-folder/`,
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
            size: '433 B',
        }, {
            name: 'demo.txt',
            path: `${pathId}demo.txt`,
            isDirectory: false,
            mode: 'drw-r--r--',
            uid: '1000',
            gid: '1000',
            date: '2021-10-10 12:00:00Z',
            size: '433 B',
        }, {
            name: 'demo.mp4',
            path: `${pathId}demo.mp4`,
            isDirectory: false,
            mode: 'drw-r--r--',
            uid: '1000',
            gid: '1000',
            date: '2021-10-10 12:00:00Z',
            size: '590 Kb',
        }, {
            name: 'demo.mp3',
            path: `${pathId}demo.mp3`,
            isDirectory: false,
            mode: 'drw-r--r--',
            uid: '1000',
            gid: '1000',
            date: '2021-10-10 12:00:00Z',
            size: '144 Kb',
        }, {
            name: 'demo-small.jpg',
            path: `${pathId}demo-small.jpg`,
            isDirectory: false,
            mode: 'drw-r--r--',
            uid: '1000',
            gid: '1000',
            date: '2021-10-10 12:00:00Z',
            size: '95 Kb',
        }, {
            name: 'demo-fat.jpeg',
            path: `${pathId}demo-fat.jpg`,
            isDirectory: false,
            mode: 'drw-r--r--',
            uid: '1000',
            gid: '1000',
            date: '2021-10-10 12:00:00Z',
            size: '300 Mb',
        },]
    }
    // return a promise
    return baseResponse;
 
}
   */

export const dummySearch = async (searchParams) => {
    return [{
        snapshot: {
            id: '123',
            shortId: '123',
            rootPath: '/home/',
        },
        date: '2021-10-10 12:00:00Z',
        type: 'file',
        path: '/home/fred.jpg',
    },
        {
        snapshot: {
            id: '123',
            shortId: '123',
            rootPath: '/home/',
        },
        date: '2021-10-10 12:00:00Z',
        type: 'folder',
        path: '/home/other-folder/',
    }]
}