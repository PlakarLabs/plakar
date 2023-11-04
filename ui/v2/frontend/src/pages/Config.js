import DefaultLayout from "../layouts/DefaultLayout";
import {Button, Container, TextField, Typography} from "@mui/material";
import React, {useEffect, useState} from "react";
import {confApp, selectConf} from "../state/Root";
import {useDispatch, useSelector} from "react-redux";
import {useNavigate} from "react-router-dom";
import {SNAPSHOT_ROUTE} from "../utils/Routes";


function Config() {
    const [apiUrl, setApiUrlLocal] = useState('');
    const dispatch = useDispatch();
    const {apiUrl: apiUrlRedux, repository: repositoryRedux} = selectConf(useSelector(state => state));
    const navigate = useNavigate();

    useEffect(() => {
        // This code will run after the component has rendered
        setApiUrlLocal(apiUrlRedux);

        if (apiUrlRedux && repositoryRedux) {
            navigate(SNAPSHOT_ROUTE);
        }

    }, [apiUrlRedux, repositoryRedux]);

    function handleSubmit(event) {
        event.preventDefault();
        dispatch(confApp(apiUrl));
        if (apiUrlRedux && repositoryRedux) {
            navigate(SNAPSHOT_ROUTE);
        }
    }

    return (
        <>
            <DefaultLayout>
                <Container sx={{padding: 4}}>
                    <Typography variant="h3" component="h1">Configuration</Typography>
                    <form onSubmit={handleSubmit}>
                        <TextField id="api-url" label="API URL" variant="standard" value={apiUrl || apiUrlRedux}
                                   onChange={event => setApiUrlLocal(event.target.value)}/>
                        <Button variant="contained" type="submit">Save</Button>
                    </form>
                </Container>
            </DefaultLayout>
        </>
    );
}

export default Config;