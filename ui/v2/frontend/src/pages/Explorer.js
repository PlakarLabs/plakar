import {useParams, useSearchParams} from "react-router-dom";
import {Link, Typography} from "@mui/material";
import PathList from "../screens/PathList";
import FileDetails from "../screens/FileDetails";
import DefaultLayout from "../layouts/DefaultLayout";

function prepareParams({snapshotId, '*': path}) {
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
    return { snapshotId, path, isDirectory };
}


function Explorer () {
    const {snapshotId, path, isDirectory} = prepareParams(useParams());

    return (
        <DefaultLayout>
            <Link>bob</Link>
            <Typography>Explorer</Typography>
            <Typography>{snapshotId}</Typography>
            <Typography>{path}</Typography>
            <Typography>is Directory ? {isDirectory ? 'Yes' : 'No'}</Typography>
            // shows FileDetails is isDirectory is false
            {isDirectory && <PathList snapshotId={snapshotId} path={path}/>}
            {!isDirectory && <FileDetails snapshotId={snapshotId} path={path}/>}

        </DefaultLayout>
    )
}

export default Explorer;