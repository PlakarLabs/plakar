import React from 'react';
import {Provider} from 'react-redux';

import {persistor, routerSelector, store, history} from "./utils/Store";
import {Route, Routes} from "react-router-dom";
import Welcome from "./pages/Welcome";
import {PersistGate} from "redux-persist/integration/react";
import {ReduxRouter} from "@lagunovsky/redux-react-router";
import Config from "./pages/Config";
import SnapshotList from "./pages/SnapshotList";
import {CONFIG_ROUTE, SNAPSHOT_ROUTE} from "./utils/Routes";
import Explorer from "./pages/Explorer";
import SearchResults from "./pages/SearchResults";


// http://localhost:3000?api_url=http://localhost:8000&store_name=plakar

function App() {
    return (
        <Provider store={store}>
            <PersistGate loading={null} persistor={persistor}>
                <ReduxRouter history={history} routerSelector={routerSelector}>
                    <Routes>
                        <Route path={'/'} element={<Welcome/>}/>
                        <Route path={'/search'} element={<SearchResults/>}/>
                        <Route path={SNAPSHOT_ROUTE} element={<SnapshotList/>}/>
                        <Route path={'snapshot/:snapshotId/*'} element={<Explorer/>}/>

                        <Route path={CONFIG_ROUTE} element={<Config/>}/>
                    </Routes>
                </ReduxRouter>
            </PersistGate>
        </Provider>

    );
}


export default App;