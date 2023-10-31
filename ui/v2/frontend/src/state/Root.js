
export const fetchInitialData = () => async dispatch => {
    dispatch({ type: 'FETCH_INITIAL_DATA_REQUEST' });
    try {
      // Fetch the initial data from the API
      const response = await fetch('/api/initial-data');
      const data = await response.json();
      // Dispatch the success action with the data
      dispatch({ type: 'FETCH_INITIAL_DATA_SUCCESS', payload: data });
    } catch (error) {
      // Dispatch the failure action with the error
      dispatch({ type: 'FETCH_INITIAL_DATA_FAILURE', error });
    }
  };

// Example action
export const fetchSnapshots = () => async dispatch => {
    dispatch({ type: 'FETCH_SNAPSHOTS_REQUESTS' });
    try {
        // sleep for 3 seconds to simluate a slow network
        await new Promise(r => setTimeout(r, 3000));
        //   const response = await fetch('/api/posts');
        //   const data = await response.json();
        const data = [
            {'uuid': '435FEAC9-7FFC-45B1-8E9C-6122DF2C953D', 'hostname': 'poolp.local'},
            {'uuid': '806C7584-2488-4F39-A639-CF617C5694C7', 'hostname': 'dummy.local'},
            {'uuid': '90B077B5-1625-484F-8B23-D6D0B2A192AF', 'hostname': 'fred.local'},
        ];
      dispatch({ type: 'FETCH_SNAPSHOTS_SUCCESS', payload: data });
    } catch (error) {
      dispatch({ type: 'FETCH_SNAPSHOTS_FAILURE', error });
    }
  };

export const confApp = (apiUrl, storeName) => async dispatch => {
    const data = { apiUrl: apiUrl, storeName: storeName }
    dispatch({ type: 'SET_CONF', payload: data});
  };

  // Example reducer
  const initialState = {
    snapshots: [],
    loading: false,
    error: null,
  };

  export const snapshotsReducer = (state = initialState, action) => {
    switch (action.type) {
      case 'FETCH_SNAPSHOTS_REQUESTS':
        return { ...state, loading: true };
      case 'FETCH_POSTS_SUCCESS':
        return { ...state, loading: false, posts: action.payload };
      case 'FETCH_SNAPSHOTS_FAILURE':
        return { ...state, loading: false, error: action.error };
      default:
        return state;
    }
  };


  const confState = {
      apiUrl: null,
    storeName: null,
  };
  export const confReducer = (state = confState, action) => {
    switch (action.type) {
      case 'SET_CONF':
        return {...state, apiUrl: action.payload.apiUrl, storeName: action.payload.storeName};
      default:
        return state;
    }
  };

  // Example selector
  export const selectSnapshots = glState => glState.snapshots;
  export const selectConf = glState => glState.conf;