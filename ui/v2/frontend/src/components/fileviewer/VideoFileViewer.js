import {Button, Card, CardActions, CardContent, Stack, Typography} from "@mui/material";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import {materialTheme as theme} from "../../Theme";
import DownloadIcon from "@mui/icons-material/Download";
import React from "react";
import {selectFileDetails} from "../../state/Root";
import {useSelector} from "react-redux";
import {triggerDownload} from "../../utils/BrowserInteraction";
import {Image} from "theme-ui";


const VideoFileViewer = () => {

    const fileDetails = selectFileDetails(useSelector(state => state));

    const handleDownloadFile = () => {
        triggerDownload(fileDetails.rawPath, fileDetails.name);
    }

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