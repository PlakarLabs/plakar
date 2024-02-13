import {
    fetchConfig,
    fetchSnapshotsPath,
    fetchSnapshots as fetchSnapshotsPathWithApiClient,
    search as searchWithApiClient
} from "../utils/PlakarApiClient";


export const fetchSnapshots = (apiUrl, page = 1, pageSize = 10) => async dispatch => {
    dispatch({type: 'FETCH_SNAPSHOTS_REQUESTS'});
    try {
        console.log('loading snapshots...');
        await fetchSnapshotsPathWithApiClient(apiUrl, page, pageSize).then((data) => {
            dispatch({type: 'FETCH_SNAPSHOTS_SUCCESS', payload: data});
        });
    } catch (error) {
        dispatch({type: 'FETCH_SNAPSHOTS_FAILURE', error});
    }
};

const confState = {
    apiUrl: null,
    repository: null,
    pageSize: 10,
    loading: false,
    error: null,
};
export const confReducer = (state = confState, action) => {
    switch (action.type) {
        case 'SET_API_URL':
            return {...state, apiUrl: action.payload.apiUrl};
        case 'FETCH_CONF_REQUEST':
            return {...state, loading: true};
        case 'FETCH_CONF_SUCCESS':
            return {...state, loading: false, repository: action.payload.repository};
        case 'FETCH_CONF_FAILURE':
            return {...state, loading: false, error: action.error, repository: null};
        default:
            return state;
    }
};

export const confApp = (apiUrl) => async dispatch => {
    dispatch({type: 'SET_API_URL', payload: {apiUrl: apiUrl}});
    dispatch({type: 'FETCH_CONF_REQUEST'});
    try {
        // sleep for 3 seconds to simluate a slow network
        await fetchConfig(apiUrl).then((data) => {
            dispatch({type: 'FETCH_CONF_SUCCESS', payload: data});
        });
    } catch (error) {
        dispatch({type: 'FETCH_CONF_FAILURE', error});
    }
};

// Example reducer
const initialState = {
    snapshotsPage: null,
    loading: false,
    error: null,
};

export const snapshotsReducer = (state = initialState, action) => {
    switch (action.type) {
        case 'FETCH_SNAPSHOTS_REQUESTS':
            return {...state, loading: true};
        case 'FETCH_SNAPSHOTS_SUCCESS':
            return {...state, loading: false, snapshotsPage: action.payload};
        case 'FETCH_SNAPSHOTS_FAILURE':
            return {...state, loading: false, error: action.error};
        default:
            return state;
    }
};

export const selectSnapshotsPage = glState => glState.snapshots.snapshotsPage;


// Example selector
export const selectSnapshots = glState => glState.snapshots;
export const selectRepository = glState => glState.conf.repository;
export const selectConf = glState => glState.conf;
//export const selectApiUrl = glState => glState.conf.apiUrl;
export const selectPageSize = glState => glState.conf.pageSize;


export const selectSnapshot = glState => glState.pathView.snapshot;
export const selectFileDetails = glState => glState.pathView.items[0];
export const selectPathPage = glState => glState.pathView;


export const lookupFileDetails = (glState, path) => {
    console.log('lookupFileDetails', glState.pathView.items);
    return glState.pathView.items.find((item) => {
        console.log('lookupFileDetails', item.path, path);
        return item.path === path
    });
};


const pathViewState = {
    snapshot: {
        id: null,
    },
    path: null,
    items: [],
    page: 1,
    pageSize: 10,
    totalPages: 1,
    loading: false,
    error: null,
}

export const pathViewReducer = (state = pathViewState, action) => {
        switch (action.type) {
            case 'FETCH_PATH_REQUEST':
                return {
                    ...state,
                    loading: true,
                    snapshot: {id: action.payload.snapshotId},
                    items: [],
                    path: action.payload.path,
                    page: action.payload.page,
                    pageSize: action.payload.pageSize,
                };
            case 'FETCH_PATH_SUCCESS':
                return {
                    ...state,
                    loading: false,
                    snapshot: action.payload.snapshot,
                    path: action.payload.path,
                    items: action.payload.items,
                    page: action.payload.page,
                    pageSize: action.payload.pageSize,
                    totalPages: action.payload.totalPages,
                }
            case 'FETCH_PATH_FAILURE':
                return {...state, loading: false, error: action.error, items: []};
            default:
                return state;
        }
    }
;

export const fetchPath = (snapshotId, path, pageOffset = 1, pageSize = 10) => async dispatch => {
    dispatch({type: 'FETCH_PATH_REQUEST', payload: {snapshotId, path, pageOffset, pageSize}});
    try {
        await fetchSnapshotsPath(`${snapshotId}:${path}`, pageOffset, pageSize).then(
            (page) => {
            dispatch({type: 'FETCH_PATH_SUCCESS', payload: page});
        });
    } catch (error) {
        console.log('Error:', error);
        dispatch({type: 'FETCH_PATH_FAILURE', error});
    }
};

const searchState = {
    items: [],
    searchParams: '',
    loading: false,
    error: null,
}

export const searchReducer = (state = searchState, action) => {
    switch (action.type) {
        case 'SEARCH_REQUEST':
            return {...state, loading: true, items: [], searchParams: action.searchParams};
        case 'SEARCH_SUCCESS':
            return {
                ...state,
                loading: false,
                items: action.payload,
            }
        case 'SEARCH_FAILURE':
            return {...state, loading: false, error: action.error};
        default:
            return state;
    }
};

export const search = (searchParams) => async dispatch => {
    dispatch({type: 'SEARCH_REQUEST', searchParams: searchParams});
    try {
        // sleep for 3 seconds to simluate a slow network
        await searchWithApiClient('', searchParams).then((searchResult) => {
            setTimeout(() => {
                dispatch({type: 'SEARCH_SUCCESS', payload: searchResult});
            }, 3000);

        });
    } catch (error) {
        console.log('Error:', error);
        dispatch({type: 'SEARCH_FAILURE', error});
    }
}

export const selectSearchResult = glState => glState.search.items;
export const selectSearchParams = glState => glState.search.searchParams;
export const selectSearchLoading = glState => glState.search.loading;