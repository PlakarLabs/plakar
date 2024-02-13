export function fetchConfig() {
    return fetch('/api/config')
        .then(response => response.json())
        .catch(error => alert(error));
}

export function fetchSnapshots(page, pageSize) {
    return fetch('/api/snapshots?offset=' + (page-1) * pageSize + '&limit=' + pageSize)
        .then(response => response.json())
        .catch(error => alert(error));
}

export async function fetchSnapshotsPath(pathId, page, pageSize) {
    return fetch('/api/snapshot/'+ pathId +'?offset=' + (page-1) * pageSize + '&limit=' + pageSize)
        .then(response => response.json())
        .catch(error => alert(error));
}

export function search(searchParams) {
    return fetch('/api/search?q=' + searchParams)
        .then(response => response.json())
        .catch(error => alert(error));
}