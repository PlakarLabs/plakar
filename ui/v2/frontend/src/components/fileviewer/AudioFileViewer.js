import {Stack} from "@mui/material";
import React from "react";
import {selectFileDetails} from "../../state/Root";
import {useSelector} from "react-redux";


const VideoFileViewer = () => {

    const fileDetails = selectFileDetails(useSelector(state => state));

    return (
        <Stack alignItems={'center'} padding={2}>
            <audio controls>
                <source src={fileDetails.rawPath} type="audio/mp3"/>
                Your browser does not support the audio element.
            </audio>
        </Stack>
    )
}

export default VideoFileViewer;