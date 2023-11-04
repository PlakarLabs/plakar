import React, {useEffect} from 'react';

import {confApp, selectConf} from "../state/Root";
import {useDispatch, useSelector} from "react-redux";
import DefaultLayout from "../layouts/DefaultLayout";
import {useNavigate, useSearchParams} from "react-router-dom";
import {CONFIG_ROUTE, SNAPSHOT_ROUTE} from "../utils/Routes";

function Welcome(props) {
    const dispatch = useDispatch();
    const navigate = useNavigate();
    let [searchParams] = useSearchParams();
    const {apiUrl: apiUrlRedux, repository: repositoryRedux} = selectConf(useSelector(state => state));

    useEffect(() => {

        if (apiUrlRedux && repositoryRedux) {
            navigate(SNAPSHOT_ROUTE);
        } else {
            // Get the URLSearchParams object from the current URL
            // const searchParams = new URLSearchParams(window.location.search);

            // Get the value of the 'api' parameter
            const apiUrl = searchParams.get('api_url');

            // Store the 'api' value in local storage
            if (apiUrl != null) {
                dispatch(confApp(apiUrl)).then(() => {
                    console.log('dispatched conf app');
                    navigate(SNAPSHOT_ROUTE)
                });

                // const newUrl = window.location.pathname + window.location.hash;
                // window.history.replaceState({}, '', newUrl);
            } else {
                navigate(CONFIG_ROUTE);
            }
        }
        // Remove the 'api_url' parameter from the URL

    }, [apiUrlRedux, dispatch, navigate, searchParams, repositoryRedux]);

    return (
        <DefaultLayout>
            redirecting...
        </DefaultLayout>
    );
}

export default Welcome;