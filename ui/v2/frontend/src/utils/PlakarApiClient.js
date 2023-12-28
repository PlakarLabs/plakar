export function fetchConfig(apiUrl) {
    apiUrl = 'http://localhost:3010';
    return fetch(apiUrl+'/api/config')
        .then(response => response.json())
        .catch(error => alert(error));
}

export function fetchSnapshots(apiUrl, page, pageSize) {
    apiUrl = 'http://localhost:3010';
    return fetch(apiUrl+'/api/snapshots?offset=' + (page-1) * pageSize + '&limit=' + pageSize)
        .then(response => response.json())
        .catch(error => alert(error));
}

export async function fetchSnapshotsPath(apiUrl, pathId, page, pageSize) {
    apiUrl = 'http://localhost:3010';
    return fetch(apiUrl+'/api/snapshot/'+ pathId +'?offset=' + (page-1) * pageSize + '&limit=' + pageSize)
        .then(response => response.json())
        .catch(error => alert(error));
}


export function search(apiUrl, searchParams) {
    apiUrl = 'http://localhost:3010';

    return fetch(apiUrl+'/api/search?q=' + searchParams)
        .then(response => response.json())
        .catch(error => alert(error));
}