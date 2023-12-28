import React, {useMemo} from 'react';
import {Stack} from "@mui/material";
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
            <audio controls>
                <source src={fileDetails.rawPath} type={fileDetails.mimeType} />
                Your browser does not support the audio element.
            </audio>
        </Stack>
    )
}

export default VideoFileViewer;