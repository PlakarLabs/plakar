import {legacy_createStore as createStore, applyMiddleware, combineReducers, compose} from 'redux';
import thunk from 'redux-thunk';
import {persistStore, persistReducer} from 'redux-persist';
import storage from 'redux-persist/lib/storage';
import {reducer as formReducer} from 'redux-form';
import {snapshotsReducer, confReducer, pathViewReducer} from '../state/Root';
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
        navigator: createRouterReducer(history)
    }
);

const persistConfig = {
    key: 'plakar_state',
    storage,
};

const persistedReducer = persistReducer(persistConfig, rootReducer);

const enhancers = process.env.NODE_ENV === 'development' ?
    compose(
        applyMiddleware(createRouterMiddleware(history), thunk),
        // createRouterMiddleware(history),
        composeWithDevTools(),
    )
    : compose(
        applyMiddleware(thunk),
        createRouterMiddleware(history),
    );

export const store = createStore(persistedReducer, enhancers);
export const persistor = persistStore(store);
export const routerSelector = (state) => state.navigator