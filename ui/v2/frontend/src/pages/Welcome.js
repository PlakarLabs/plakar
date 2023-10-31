import React, {useEffect} from 'react';

import {confApp, fetchSnapshots, selectConf, selectSnapshots} from "../state/Root";
import {connect, useDispatch, useSelector} from "react-redux";
import DefaultLayout from "../layouts/DefaultLayout";
import {useNavigate, useSearchParams} from "react-router-dom";
import {CONFIG_ROUTE, SNAPSHOT_ROUTE} from "../utils/Routes";

function Welcome(props) {
    const dispatch = useDispatch();
    const navigate = useNavigate();
    let [searchParams, setSearchParams] = useSearchParams();
    const {apiUrlRedux: apiUrlRedux, storageNameRedux: storageNameRedux} = selectConf(useSelector(state => state));


    useEffect(() => {

        if (apiUrlRedux && storageNameRedux) {
            navigate('/snapshots');
        } else {
            // Get the URLSearchParams object from the current URL
            // const searchParams = new URLSearchParams(window.location.search);

            // Get the value of the 'api' parameter
            const apiUrl = searchParams.get('api_url');
            const storeName = searchParams.get('store_name');

            // Store the 'api' value in local storage
            if (apiUrl != null && storeName != null) {
                dispatch(confApp(apiUrl, storeName));
                navigate(SNAPSHOT_ROUTE);
                // const newUrl = window.location.pathname + window.location.hash;
                // window.history.replaceState({}, '', newUrl);
            } else {
                navigate(CONFIG_ROUTE);
            }
        }
        // Remove the 'api_url' parameter from the URL

    }, [confApp]);

    return (
        <DefaultLayout>
            redirecting...
        </DefaultLayout>
    );
}

export default Welcome;