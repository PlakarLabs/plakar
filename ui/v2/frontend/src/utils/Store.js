import {legacy_createStore as createStore, applyMiddleware, combineReducers, compose} from 'redux';
import thunk from 'redux-thunk';
import {persistStore, persistReducer} from 'redux-persist';
import storage from 'redux-persist/lib/storage';
import {reducer as formReducer} from 'redux-form';
import {snapshotsReducer, confReducer, pathViewReducer, searchReducer} from '../state/Root';
import {composeWithDevTools} from 'redux-devtools-extension';
import {createBrowserHistory} from 'history'
import {createRouterMiddleware, createRouterReducer} from "@lagunovsky/redux-react-router";

export const history = createBrowserHistory()

const rootReducer = combineReducers({
        // Add your reducers here
        form: formReducer,
        snapshots: snapshotsReducer,
        pathView: pathViewReducer,
        conf: confReducer,
        navigator: createRouterReducer(history),
        search: searchReducer,
    }
);

const persistConfig = {
    key: 'plakar_state',
    storage,
};

const persistedReducer = persistReducer(persistConfig, rootReducer);


const enhancers = process.env.REACT_APP_DEV_TOOLS_ENABLED === 'true' ?
    compose(
        applyMiddleware(createRouterMiddleware(history), thunk),
        // createRouterMiddleware(history),
        composeWithDevTools(),
    )
    : compose(
        applyMiddleware(createRouterMiddleware(history), thunk),
    );
let s = null;
try {
    s = createStore(persistedReducer, {}, enhancers);
} catch (error) {
    console.log('Error creating store', error);
    if (process.env.REACT_APP_DEV_TOOLS_ENABLED) {
        console.warn('Warning: if you get some error creating your store, make sure you have installed redux dev tools extension');
    }
}
export const store = s;
export const persistor = persistStore(store);

// XXX - debug remove state from local storage, it's painful when working on the app
persistor.purge();


export const routerSelector = (state) => state.navigator