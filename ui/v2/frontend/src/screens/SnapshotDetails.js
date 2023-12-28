import React from 'react';
import {Link, Stack, Typography} from "@mui/material";
import TitleSubtitle from "../components/TitleSubtitle";
import TagList from "../components/TagList";
import {selectSnapshot} from "../state/Root";
import {shallowEqual, useSelector} from "react-redux";
import {Link as RouterLink} from "react-router-dom";
import {topDirectoryURL} from "../utils/Routes";


const SnapshotDetails = () => {
    const snapshot = useSelector(selectSnapshot, shallowEqual);

    return (
        <Stack spacing={2} padding={3}>
            <Typography variant={'textlgmedium'}>Details</Typography>
            <TitleSubtitle subtitle={snapshot.id} title={"Snapshot Id"}/>
            <TitleSubtitle subtitle={snapshot.location || 'unknown'} title={"Location"}/>
            <TitleSubtitle subtitle={snapshot.date} title={"Snapshot Date"}/>
            <TitleSubtitle subtitle={<Link component={RouterLink} to={topDirectoryURL(snapshot.id, snapshot.rootPath + '/')}>{snapshot.rootPath}</Link>} title={"Top Directory"}/>
            <TitleSubtitle subtitle={snapshot.os} title={"Operating System"}/>
            <TitleSubtitle subtitle={snapshot.username} title={"Username"}/>
            <TitleSubtitle subtitle={snapshot.hostName} title={"Hostname"}/>

            {/* Conditionally display signature if any */}
            {snapshot.signature && (
                <TitleSubtitle subtitle={snapshot.signature} title={"Signature"}/>
            )}
        
            {/* Conditionally render the Tags stack */}
            {snapshot.tags && snapshot.tags.length > 0 && (
                <Stack>
                    <Typography variant={'textbasemedium'}>Tags</Typography>
                    <TagList tags={snapshot.tags}/>
                </Stack>
            )}

        </Stack>
    )
}

export default SnapshotDetails;