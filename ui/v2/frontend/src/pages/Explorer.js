import {useParams, useSearchParams} from "react-router-dom";
import {Breadcrumbs, Grid, Link, Stack, Typography} from "@mui/material";
import PathList from "../screens/PathList";
import FileDetails from "../screens/FileDetails";
import DefaultLayout from "../layouts/DefaultLayout";
import TwoColumnLayout from "../layouts/TwoColumnLayout";
import TitleSubtitle from "../components/TitleSubtitle";
import Tag from "../components/Tag";
import TagList from "../components/TagList";

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

    return (
        <TwoColumnLayout leftComponent={<>
            {isDirectory && <PathList snapshotId={snapshotId} path={path}/>}
            {!isDirectory && <FileDetails snapshotId={snapshotId} path={path}/>}
        </>}
                         rightComponent={<Stack spacing={2} padding={3}>
                             <Typography variant={'textlgmedium'}>Details</Typography>
                             <TitleSubtitle/>
                             <TitleSubtitle/>
                             <TitleSubtitle/>
                             <TitleSubtitle/>
                             <TitleSubtitle/>
                             <Stack>
                                 <Typography variant={'textbasemedium'}>Snapshot Id</Typography>
                                 <TagList tags={['fred', 'bob', 'hello',]}/>
                             </Stack>

                         </Stack>}
        >


        </TwoColumnLayout>
    )
}

export default Explorer;