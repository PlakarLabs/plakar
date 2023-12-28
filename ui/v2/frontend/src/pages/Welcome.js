import React, {useEffect} from 'react';

import {confApp, selectApiUrl, selectRepository} from "../state/Root";
import {shallowEqual, useDispatch, useSelector} from "react-redux";
import DefaultLayout from "../layouts/DefaultLayout";
import {useNavigate} from "react-router-dom";
import {SNAPSHOT_ROUTE} from "../utils/Routes";

function Welcome(props) {
    const dispatch = useDispatch();
    const navigate = useNavigate();
    const apiUrlRedux = useSelector(selectApiUrl, shallowEqual);
    const repositoryRedux = useSelector(selectRepository, shallowEqual);

    useEffect(() => {
        navigate(SNAPSHOT_ROUTE);

        if (apiUrlRedux && repositoryRedux) {
            navigate(SNAPSHOT_ROUTE);
        } else {
            // Set the API URL directly
            const fixedApiUrl = 'http://localhost:3010';

            // Dispatch the fixed API URL
            dispatch(confApp(fixedApiUrl));
        }

    }, [apiUrlRedux, dispatch, navigate, repositoryRedux]);

    return (
        <DefaultLayout>
            redirecting...
        </DefaultLayout>
    );
}

export default Welcome;