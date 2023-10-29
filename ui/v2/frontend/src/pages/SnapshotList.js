// basic react function for a component

import React from 'react';
import { connect } from 'react-redux';
import {useState, useEffect} from 'react';
import {useDispatch, useSelector} from 'react-redux';
import {selectSnapshots, fetchSnapshots} from '../state/Root';
import {Typography, Stack, AppBar, Container} from '@mui/material';
import Tag from './components/Tag'

function SnapshotList({apiUrl, snapshots, fetchSnapshots}) {

    useEffect(() => {
            fetchSnapshots();

        },

        [fetchSnapshots]);


    return (
        <Stack>

            <AppBar position="static" color="transparent">
                <Container maxWidth="xl" sx={{padding: 2}}>
                    <Logo padding="s"/>
                    <Typography href="#">on {apiUrl ? apiUrl : 'loading...'}</Typography>
                </Container>
            </AppBar>


            <Typography>Loading...</Typography>


            <Tag/>


        </Stack>
    );

};


const mapStateToProps = state => ({
    snapshots: selectSnapshots(state),
});

const mapDispatchToProps = {
    fetchSnapshots,
};

export default connect(mapStateToProps, mapDispatchToProps);