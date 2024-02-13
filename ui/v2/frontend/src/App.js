import React from 'react';
import {Provider} from 'react-redux';

import {persistor, routerSelector, store, history} from "./utils/Store";
import {Route, Routes} from "react-router-dom";
import {PersistGate} from "redux-persist/integration/react";
import {ReduxRouter} from "@lagunovsky/redux-react-router";
import Snapshots from "./pages/Snapshots";
import Explorer from "./pages/Explorer";
//import SearchResults from "./pages/SearchResults";


//<Route path={'/search'} element={<SearchResults/>}/>
function App() {
    return (
        <Provider store={store}>
            <PersistGate loading={null} persistor={persistor}>
                <ReduxRouter history={history} routerSelector={routerSelector}>
                    <Routes>
                        <Route path={'/'} element={<Snapshots />}/>
                        <Route path={'/snapshot/:snapshotId/*'} element={<Explorer/>}/>
                    </Routes>
                </ReduxRouter>
            </PersistGate>
        </Provider>

    );
}

export default App;