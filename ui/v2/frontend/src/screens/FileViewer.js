import {useParams, useSearchParams} from "react-router-dom";
import FileBreadcrumbs from "../components/FileBreadcrumb";
import {
    Button, Card,
    CardActions,
    CardContent, CircularProgress,
    Stack,
    Typography
} from "@mui/material";

import React, {useCallback, useState} from "react";

import {confApp, fetchPath, selectApiUrl, selectFileDetails} from "../state/Root";
import {connect, shallowEqual, useDispatch, useSelector} from "react-redux";
import UnsupportedFileViewer from "../components/fileviewer/UnsupportedFileViewer";
import TextFileViewer from "../components/fileviewer/TextFileViewer";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import {materialTheme as theme} from "../Theme";
import DownloadIcon from "@mui/icons-material/Download";
import VisibilityIcon from '@mui/icons-material/Visibility';
import {triggerDownload} from "../utils/BrowserInteraction";
import ImageFileViewer from "../components/fileviewer/ImageFileViewer";
import VideoFileViewer from "../components/fileviewer/VideoFileViewer";
import AudioFileViewer from "../components/fileviewer/AudioFileViewer";
import {getDirectoryPath, getFileName} from "../utils/Path";


// how to imple,e hightlighting
// https://blog.logrocket.com/guide-syntax-highlighting-react/

// 10 MB
const PREVIEW_FROM_SIZE = 10485760;

function FileDetails({snapshotId, path, fileDetails}) {
    const dispatch = useDispatch();
    let {id} = useParams();
    let [searchParams] = useSearchParams();
    //let [preview, setPreview] = useState(false);
    let [preview, setPreview] = useState(false);
    const apiUrl = useSelector(selectApiUrl, shallowEqual);

    React.useEffect(() => {
        dispatch(fetchPath(apiUrl, snapshotId, path, 1, 1));
    }, [dispatch, apiUrl, snapshotId, path]);

    const handlePreview = useCallback(() => {
        setPreview(true);
    }, [setPreview]);

    const handleDownloadFile = useCallback(() => {
        triggerDownload(fileDetails.rawPath, fileDetails.name);
    }, [fileDetails]);

    return (<>
            <Typography variant="h3" component="h1">{getFileName(path)}</Typography>
            {id}
            {searchParams.get('p')}
            <FileBreadcrumbs path={`${getDirectoryPath(path)}/`} snapshotid={snapshotId}/>
            <Stack alignItems={'center'}>
                {!fileDetails && <CircularProgress />}
            </Stack>

            {fileDetails && fileDetails.byteSize > PREVIEW_FROM_SIZE && !preview &&
            <Stack alignItems={'center'} padding={2}>
                <Card variant="outlined" sx={{
                    width: '474px',
                    height: '224px',
                    boxShadow: "0px 25px 50px 0px rgba(31, 41, 55, 0.25)",
                    border: 0,
                    borderRadius: 2
                }}>
                    <CardContent>
                        <Stack alignItems={'center'} spacing={1} pt={1}>
                            <InfoOutlinedIcon/>
                            <Typography variant="textlgmedium" component="h2">This is a very large file...</Typography>
                            <Typography variant='textsmregular' color={theme.palette.gray[500]}>The preview has been
                                disabled to prevent unexpected performance issues. If you wish to continue, no
                                problem.</Typography>
                        </Stack>

                    </CardContent>
                    <CardActions>
                        <Stack sx={{flex: 1}} alignItems='center'>
                            <Stack direction={'row'} spacing={2}>
                                <Button size="large" color="primary" variant={'outlined'} endIcon={<VisibilityIcon/>}
                                onClick={handlePreview}>
                                    <Typography variant={'textsmregular'}>Preview Anyway</Typography>
                                </Button>
                                <Button size="large" color="primary" variant={'contained'} endIcon={<DownloadIcon/>}
                                onClick={handleDownloadFile}
                                >
                                    <Typography variant={'textsmregular'}>Download Raw File</Typography>
                                </Button>
                            </Stack>
                        </Stack>
                    </CardActions>
                </Card>
            </Stack>
            }


            {(fileDetails && (fileDetails.byteSize < PREVIEW_FROM_SIZE || (fileDetails.byteSize > PREVIEW_FROM_SIZE && preview ))) && (() => {
                switch (fileDetails.mimeType) {
                    case 'text/javascript':
                    case 'text/plain':
                        return <TextFileViewer/>
                    case 'image/jpeg':
                        return <ImageFileViewer />
                    case 'video/mp4':
                        return <VideoFileViewer />
                    case 'audio/mp3':
                        return <AudioFileViewer />
                    default:
                        return <UnsupportedFileViewer/>
                }
            })()}
        </>
    )
}

const mapStateToProps = state => ({
    fileDetails: selectFileDetails(state),
});

const mapDispatchToProps = {
    confApp,
};

export default connect(mapStateToProps, mapDispatchToProps)(FileDetails);