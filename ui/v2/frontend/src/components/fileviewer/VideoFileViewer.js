import {Stack} from "@mui/material";
import React from "react";
import {useMemo} from 'react';
import {useSelector} from "react-redux";

import {prepareParams} from "../../pages/Explorer";
import {useParams} from "react-router-dom";
import {lookupFileDetails} from "../../state/Root";

const VideoFileViewer = () => {
    const params = useParams();
    const {snapshotId, path} = useMemo(() => prepareParams(params), [params]);
    const fileDetails = useSelector(state => lookupFileDetails(state, snapshotId+":"+path));


    return (
        <Stack alignItems={'center'} padding={2}>
            <video width="320" height="240" controls>
                <source src={fileDetails.rawPath} type={fileDetails.mimeType}/>
                Your browser does not support the video tag.
            </video>
        </Stack>
    )
}

export default VideoFileViewer;