import {Button, Card, CardActions, CardContent, Stack, Typography} from "@mui/material";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import {materialTheme as theme} from "../../Theme";
import DownloadIcon from "@mui/icons-material/Download";
import React, {useMemo} from 'react';
import {useSelector} from "react-redux";
import {triggerDownload} from "../../utils/BrowserInteraction";
import {useParams} from "react-router-dom";
import { prepareParams } from "../../pages/Explorer";
import {lookupFileDetails} from "../../state/Root";


const UnsupportedFileViewer = () => {
    const params = useParams();
    const {snapshotId, path} = useMemo(() => prepareParams(params), [params]);
    const fileDetails = useSelector(state => lookupFileDetails(state, snapshotId+":"+path));

    const handleDownloadFile = () => {
        triggerDownload(fileDetails.rawPath + "?download=true", fileDetails.name);
    }

    return (
        <Stack alignItems={'center'} padding={2}>
            <Card variant="outlined" sx={{
                width: '424px',
                height: '224px',
                boxShadow: "0px 25px 50px 0px rgba(31, 41, 55, 0.25)",
                border: 0,
                borderRadius: 2
            }}>
                <CardContent>
                    <Stack alignItems={'center'} spacing={1} pt={1}>
                        <InfoOutlinedIcon/>
                        <Typography variant="textlgmedium" component="h2">Preview unvailable</Typography>
                        <Typography variant='textsmregular' color={theme.palette.gray[500]}>Sorry, we don’t know how to
                            interpret this file.</Typography>
                    </Stack>

                </CardContent>
                <CardActions>
                    <Stack sx={{flex: 1}} alignItems='center'>
                        <Button size="large" color="primary" variant={'contained'} endIcon={<DownloadIcon/>} onClick={handleDownloadFile}>
                            <Typography variant={'textsmregular'}>Download Raw File</Typography>
                        </Button>
                    </Stack>
                </CardActions>
            </Card>
        </Stack>
    )
}

export default UnsupportedFileViewer;