import React, {useEffect, useMemo} from 'react';
import {Typography, Stack, Link} from '@mui/material';
import {Link as RouterLink, useParams} from "react-router-dom";
import {prepareParams} from "../pages/Explorer";

import TitleSubtitle from "../components/TitleSubtitle";
import {lookupFileDetails, selectSnapshot} from "../state/Root";
import {shallowEqual, useSelector} from "react-redux";
import {getDirectoryPath} from "../utils/Path";
import {snapshotURL} from "../utils/Routes";

const FileDetails = () => {
    const params = useParams();
    const {snapshotId, path} = useMemo(() => prepareParams(params), [params]);

    const snapshot = useSelector(selectSnapshot, shallowEqual);
    const fileDetails = useSelector(state => lookupFileDetails(state, snapshotId+":"+path));

    useEffect(() => {
        console.log('FileDetails useEffect', {snapshotId, path});
    },[snapshotId, path]);

    return (
        <Stack spacing={2} padding={3}>
            <Typography variant={'textlgmedium'}>Details</Typography>
            <TitleSubtitle subtitle={snapshot && snapshot.id} title={"Snapshot Id"}/>
            <TitleSubtitle subtitle={<Link component={RouterLink} to={fileDetails && snapshotURL(snapshot.id, `${getDirectoryPath(path)}/`)}>{fileDetails && `${getDirectoryPath(path)}/`}</Link>} title={"Parent Directory"}/>
            <TitleSubtitle title={"Checksum"} subtitle={fileDetails && fileDetails.checksum}/>
            <TitleSubtitle title={"Content Type"} subtitle={(fileDetails && fileDetails.mimeType) || "unknown"}/>
            <TitleSubtitle title={"Size"} subtitle={fileDetails && fileDetails.size}/>
            <TitleSubtitle title={"Modification Time"} subtitle={fileDetails && fileDetails.modificationTime}/>
            <TitleSubtitle title={"Mode"} subtitle={fileDetails && fileDetails.mode}/>
            <TitleSubtitle title={"Uid"} subtitle={fileDetails && fileDetails.uid}/>
            <TitleSubtitle title={"Gid"} subtitle={fileDetails && fileDetails.gid}/>
            <TitleSubtitle title={"Device"} subtitle={fileDetails && fileDetails.device}/>
            <TitleSubtitle title={"Inode"} subtitle={fileDetails && fileDetails.inode}/>

        </Stack>
    )
}

export default FileDetails;
