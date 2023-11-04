import {Link, Stack, Typography} from "@mui/material";
import TitleSubtitle from "../components/TitleSubtitle";
import TagList from "../components/TagList";
import {selectSnapshot} from "../state/Root";
import {useSelector} from "react-redux";
import {Link as RouterLink} from "react-router-dom";


const SnapshotDetails = ({navigation}) => {
    const snapshot = selectSnapshot(useSelector(state => state));

    return (
        <Stack spacing={2} padding={3}>
            <Typography variant={'textlgmedium'}>Details</Typography>
            <TitleSubtitle subtitle={snapshot && snapshot.id} title={"Snapshot Id"}/>
            <TitleSubtitle subtitle={snapshot && snapshot.location} title={"Location"}/>
            <TitleSubtitle subtitle={snapshot && snapshot.date} title={"Snapshot Date"}/>
            <TitleSubtitle subtitle={snapshot && <Link component={RouterLink} to={`/snapshot/${snapshot.id}:${snapshot.rootPath}/`}>{snapshot.rootPath}</Link>} title={"Top Directory"}/>
            <TitleSubtitle subtitle={snapshot && snapshot.os} title={"OS"}/>
            <TitleSubtitle subtitle={snapshot && snapshot.signature} title={"Signature"}/>
            <Stack>
                <Typography variant={'textbasemedium'}>Tags</Typography>
                <TagList tags={snapshot && snapshot.tags}/>
            </Stack>

        </Stack>
    )
}

export default SnapshotDetails;