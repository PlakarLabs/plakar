import {Stack} from "@mui/material";
import React from "react";
import {selectFileDetails} from "../../state/Root";
import {useSelector} from "react-redux";
import {Image} from "theme-ui";


const ImageFileViewer = () => {

    const fileDetails = selectFileDetails(useSelector(state => state));

    return (
        <Stack alignItems={'center'} padding={2}>
            <Image src={fileDetails.rawPath} sx={{width: '100%', height: '100%'}} alt={fileDetails.name}/>
        </Stack>
    )
}

export default ImageFileViewer;