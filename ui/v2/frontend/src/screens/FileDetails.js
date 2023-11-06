import {Stack, Typography} from "@mui/material";
import TitleSubtitle from "../components/TitleSubtitle";
import {selectFileDetails, selectSnapshot} from "../state/Root";
import {shallowEqual, useSelector} from "react-redux";


const FileDetails = () => {
    const snapshot = useSelector(selectSnapshot, shallowEqual);
    const fileDetails = useSelector(selectFileDetails, shallowEqual);

    return (
        <Stack spacing={2} padding={3}>
            <Typography variant={'textlgmedium'}>Details</Typography>
            <TitleSubtitle subtitle={snapshot && snapshot.id} title={"Snapshot Id"}/>
            <TitleSubtitle title={"Checksum"} subtitle={fileDetails && fileDetails.checksum}/>
            <TitleSubtitle title={"Content Type"} subtitle={fileDetails && fileDetails.mimeType}/>
            <TitleSubtitle title={"Size"} subtitle={fileDetails && fileDetails.size}/>
            <TitleSubtitle title={"Modification Date"} subtitle={fileDetails && fileDetails.modificationDate}/>
            <TitleSubtitle title={"Mode"} subtitle={fileDetails && fileDetails.mode}/>
            <TitleSubtitle title={"Uid"} subtitle={fileDetails && fileDetails.uid}/>
            <TitleSubtitle title={"Gid"} subtitle={fileDetails && fileDetails.gid}/>
            <TitleSubtitle title={"Device"} subtitle={fileDetails && fileDetails.device}/>
            <TitleSubtitle title={"Inode"} subtitle={fileDetails && fileDetails.inode}/>

        </Stack>
    )
}

export default FileDetails;