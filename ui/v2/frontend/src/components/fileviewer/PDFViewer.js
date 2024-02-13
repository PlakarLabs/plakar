import React, { useMemo } from 'react';
import { Stack } from "@mui/material";
import { useSelector } from "react-redux";
import { useParams } from "react-router-dom";

import { prepareParams } from "../../pages/Explorer";
import { lookupFileDetails } from "../../state/Root";

const PDFViewer = () => {
    const params = useParams();
    const { snapshotId, path } = useMemo(() => prepareParams(params), [params]);
    const fileDetails = useSelector(state => lookupFileDetails(state, snapshotId + ":" + path));

    return (
        <Stack alignItems={'center'} padding={2}>
            {/* Check if fileDetails is available */}
            {fileDetails && fileDetails.rawPath ? (
                <iframe
                    title={fileDetails.name}
                    src={fileDetails.rawPath}
                    type="application/pdf"
                    width="100%"
//                    height="auto"
                    style={{ width: '100vw', height: '100vh' }}
                >
                    Your browser does not support PDFs. Please download the PDF to view it.
                </iframe>
            ) : (
                <div>Loading PDF...</div>
            )}
        </Stack>
    )
}

export default PDFViewer;
