import {Stack} from "@mui/material";
import React from "react";
import {Image} from "theme-ui";
import {useMemo} from 'react';
import {useSelector} from "react-redux";

import {prepareParams} from "../../pages/Explorer";
import {useParams} from "react-router-dom";
import {lookupFileDetails} from "../../state/Root";

const ImageFileViewer = () => {
    const params = useParams();
    const { snapshotId, path } = useMemo(() => prepareParams(params), [params]);
    const fileDetails = useSelector(state => lookupFileDetails(state, snapshotId + ":" + path));

    return (
        <Stack alignItems={'center'} padding={2}>
            <Image src={fileDetails.rawPath} sx={{width: '100%', height: '100%'}} alt={fileDetails.name}/>
        </Stack>
    )
}

export default ImageFileViewer;