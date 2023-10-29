import {legacy_createStore as createStore, applyMiddleware, combineReducers, compose} from 'redux';
import thunk from 'redux-thunk';
import {persistStore, persistReducer} from 'redux-persist';
import storage from 'redux-persist/lib/storage';
import {reducer as formReducer} from 'redux-form';
import {snapshotsReducer, confReducer} from '../state/Root';
import {composeWithDevTools} from 'redux-devtools-extension';


const rootReducer = combineReducers({
    // Add your reducers here
    form: formReducer,
    snapshots: snapshotsReducer,
    conf: confReducer,
});

const persistConfig = {
    key: 'plakar_state',
    storage,
};

const persistedReducer = persistReducer(persistConfig, rootReducer);

const enhancers = process.env.NODE_ENV === 'development' ? compose(
    applyMiddleware(thunk),
    composeWithDevTools()
) : applyMiddleware(thunk);

export const store = createStore(persistedReducer,
    enhancers
);
export const persistor = persistStore(store);