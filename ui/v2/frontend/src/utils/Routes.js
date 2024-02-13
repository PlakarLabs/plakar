export const topDirectoryURL = (snapshotId, path='/') => `/snapshot/${snapshotId}:${path}`;

export const snapshotListPageURL= (page=1, pageSize=10) => {
    if (page === 1 && pageSize === 10) {
        return `/`
    } else if (page === 1) {
        return `/?pageSize=${pageSize}`
    } else if (pageSize === 10) {
        return `/?page=${page}`
    } else {
        return `/?page=${page}&pageSize=${pageSize}`
    }
};

export const snapshotURL = (snapshotId, path='/', page=1, pageSize=10) => {
    if (page === 1 && pageSize === 10) {
        return `/snapshot/${snapshotId}:${path}`
    } else if (page === 1) {
        return `/snapshot/${snapshotId}:${path}?pageSize=${pageSize}`
    } else if (pageSize === 10) {
        return `/snapshot/${snapshotId}:${path}?page=${page}`
    } else {
        return `/snapshot/${snapshotId}:${path}?page=${page}&pageSize=${pageSize}`
    }
};

export const directoryURL = (path, page=1, pageSize=10) => {
    if (page === 1 && pageSize === 10) {
        return `/snapshot/${path}`
    } else if (page === 1) {
        return `/snapshot/${path}?pageSize=${pageSize}`
    } else if (pageSize === 10) {
        return `/snapshot/${path}?page=${page}`
    } else {
        return `/snapshot/${path}?page=${page}&pageSize=${pageSize}`
    }
};