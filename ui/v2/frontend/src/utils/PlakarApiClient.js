export function fetchConfig() {
    let apiUrl = 'http://localhost:3010';
    return fetch(apiUrl+'/api/config')
        .then(response => response.json())
        .catch(error => alert(error));
}

export function fetchSnapshots(page, pageSize) {
    let apiUrl = 'http://localhost:3010';
    return fetch(apiUrl+'/api/snapshots?offset=' + (page-1) * pageSize + '&limit=' + pageSize)
        .then(response => response.json())
        .catch(error => alert(error));
}

export async function fetchSnapshotsPath(pathId, page, pageSize) {
    let apiUrl = 'http://localhost:3010';
    return fetch(apiUrl+'/api/snapshot/'+ pathId +'?offset=' + (page-1) * pageSize + '&limit=' + pageSize)
        .then(response => response.json())
        .catch(error => alert(error));
}

export function search(searchParams) {
    let apiUrl = 'http://localhost:3010';
    return fetch(apiUrl+'/api/search?q=' + searchParams)
        .then(response => response.json())
        .catch(error => alert(error));
}