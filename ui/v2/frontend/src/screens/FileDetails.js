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
            <TitleSubtitle title={"Checksum"}/>
            <TitleSubtitle title={"Content Type"}/>
            <TitleSubtitle title={"Size"}/>
            <TitleSubtitle title={"Modification Date"}/>
            <TitleSubtitle title={"Mode"}/>
            <TitleSubtitle title={"Uid"}/>
            <TitleSubtitle title={"Gid"}/>
            <TitleSubtitle title={"Device"}/>
            <TitleSubtitle title={"Inode"}/>

        </Stack>
    )
}

export default SnapshotDetails;