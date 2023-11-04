import {useParams, useSearchParams} from "react-router-dom";
import FileBreadcrumbs from "../components/FileBreadcrumb";
import {
    Button, Card,
    CardActions,
    CardContent,
    Skeleton, Stack,
    Typography
} from "@mui/material";

import React, {useState} from "react";

import {selectFileDetails} from "../state/Root";
import {useSelector} from "react-redux";
import UnsupportedFileViewer from "../components/fileviewer/UnsupportedFileViewer";
import TextFileViewer from "../components/fileviewer/TextFileViewer";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import {materialTheme as theme} from "../Theme";
import DownloadIcon from "@mui/icons-material/Download";


// how to imple hightlighting
// https://blog.logrocket.com/guide-syntax-highlighting-react/


function FileDetails({snapshotId, path}) {
    let {id} = useParams();
    let [searchParams, setSearchParams] = useSearchParams();
    const fileDetails = selectFileDetails(useSelector(state => state));
    let [preview, setPreview] = useState(false);

    React.useEffect(() => {
    }, []);

    const handlePreview = () => {
        setPreview(true);
    }

    return (<>
            <Typography variant="h3" component="h1">{fileDetails ? fileDetails.name : <Skeleton/>}</Typography>
            {id}
            {searchParams.get('p')}
            <FileBreadcrumbs path={path} snapshotid={snapshotId}/>
            {!fileDetails && <Skeleton width={'100%'} height={'800px'}/>}
            {fileDetails && fileDetails.byteSize > 300 && !preview &&
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
                                <Button size="large" color="primary" variant={'outlined'} endIcon={<DownloadIcon/>}
                                onClick={handlePreview}>
                                    <Typography variant={'textsmregular'}>Preview Anyway</Typography>
                                </Button>
                                <Button size="large" color="primary" variant={'contained'} endIcon={<DownloadIcon/>}>
                                    <Typography variant={'textsmregular'}>Download Raw File</Typography>
                                </Button>
                            </Stack>
                        </Stack>
                    </CardActions>
                </Card>
            </Stack>
            }


            {fileDetails && (fileDetails.size < 300 || preview ) && (() => {
                switch (fileDetails.mimeType) {
                    case 'text/javascript':
                        return <TextFileViewer/>
                    default:
                        return <UnsupportedFileViewer/>
                }
            })()}
        </>
    )
}


export default FileDetails;