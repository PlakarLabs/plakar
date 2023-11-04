import {useParams, useSearchParams} from "react-router-dom";
import {Breadcrumbs, Grid, Link, Stack, Typography} from "@mui/material";
import PathList from "../screens/PathList";
import FileDetails from "../screens/FileDetails";
import DefaultLayout from "../layouts/DefaultLayout";
import TwoColumnLayout from "../layouts/TwoColumnLayout";
import TitleSubtitle from "../components/TitleSubtitle";
import Tag from "../components/Tag";
import TagList from "../components/TagList";
import {useEffect} from "react";
import {useDispatch, useSelector} from 'react-redux';
import {fetchPath, selectSnapshot} from "../state/Root";
import FileViewer from "../screens/FileViewer";
import SnapshotDetails from "../screens/SnapshotDetails";

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
    return {snapshotId, path, isDirectory};
}


function Explorer() {
    const {snapshotId, path, isDirectory} = prepareParams(useParams());
    const dispatch = useDispatch()
    const snapshot = selectSnapshot(useSelector(state => state));

    useEffect(() => {
        dispatch(fetchPath({snapshotId, path, isDirectory}));
    }, [dispatch]);

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