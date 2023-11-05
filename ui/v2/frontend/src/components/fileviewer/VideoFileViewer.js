import {Stack} from "@mui/material";
import React from "react";
import {selectFileDetails} from "../../state/Root";
import {useSelector} from "react-redux";


const VideoFileViewer = () => {

    const fileDetails = selectFileDetails(useSelector(state => state));

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