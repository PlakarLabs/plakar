import React from 'react';
import {useParams} from "react-router-dom";
import PathList from "../screens/PathList";
import FileDetails from "../screens/FileDetails";
import TwoColumnLayout from "../layouts/TwoColumnLayout";
import FileViewer from "../screens/FileViewer";
import SnapshotDetails from "../screens/SnapshotDetails";
import {useEffect, useMemo} from "react";

export function prepareParams({snapshotId, '*': path}) {
    let isDirectory = false
    // remove : at end of snapshotId
    if (snapshotId.endsWith(':')) {
        snapshotId = snapshotId.slice(0, -1);
    }
    // add slash at to the path
    if (!path.startsWith('/')) {
        path = '/' + path;
    }
    // if path ends with slash, it's a directory
    if (path.endsWith('/')) {
        isDirectory = true;
    }
    return {snapshotId, path, isDirectory};
}


function Explorer() {
    let params = useParams();
    const {snapshotId, path, isDirectory} = useMemo(() => prepareParams(params), [params]);

    useEffect(() => {
        console.log('Explorer useEffect', {snapshotId, path, isDirectory});
    },[snapshotId, path, isDirectory]);

    return (
        <TwoColumnLayout leftComponent={<>
            {isDirectory && <PathList snapshotId={snapshotId} path={path}/>}
            {!isDirectory && <FileViewer snapshotId={snapshotId} path={path}/>}
        </>}
                         rightComponent={<>
                             {isDirectory && <SnapshotDetails/>}
                             {!isDirectory && <FileDetails/>}
                         </>}
        >


        </TwoColumnLayout>
    )
}

export default Explorer;