import DefaultLayout from "../layouts/DefaultLayout";
import {Button, Container, TextField, Typography} from "@mui/material";
import React, {useState} from "react";
import {confApp, selectConf} from "../state/Root";
import {connect, useDispatch} from "react-redux";
import {useNavigate} from "react-router-dom";
import {snapshotListPageURL} from "../utils/Routes";


function Config({conf}) {
    const dispatch = useDispatch();
    const navigate = useNavigate();

    const [apiUrl, setApiUrlLocal] = useState(conf.apiUrl);

    function handleSubmit(event) {
        event.preventDefault();
        dispatch(confApp(apiUrl)).then(() => {
            navigate(snapshotListPageURL(1, conf.pageSize));
        });
    }

    return (
        <>
            <DefaultLayout>
                <Container sx={{padding: 4}}>
                    <Typography variant="h3" component="h1">Configuration</Typography>
                    <form onSubmit={handleSubmit}>
                        <TextField id="api-url" label="API URL" variant="standard" value={apiUrl || conf.apiUrl}
                                   onChange={event => setApiUrlLocal(event.target.value)}/>
                        <Button variant="contained" type="submit">Save</Button>
                    </form>
                </Container>
            </DefaultLayout>
        </>
    );
}

const mapStateToProps = state => ({
    conf: selectConf(state),
});

const mapDispatchToProps = {
    confApp,
};

export default connect(mapStateToProps, mapDispatchToProps)(Config);
