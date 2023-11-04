import {Link as RouterLink, useParams, useSearchParams} from "react-router-dom";
import FileBreadcrumbs from "../components/FileBreadcrumb";
import {
    Alert,
    Box,
    Button, Card, CardActionArea, CardActions, CardContent,
    Fab,
    keyframes,
    Link, Skeleton,
    Snackbar,
    Stack,
    TextareaAutosize,
    Tooltip,
    Typography
} from "@mui/material";
import {Prism as SyntaxHighlighter} from 'react-syntax-highlighter';
import {a11yDark} from 'react-syntax-highlighter/dist/esm/styles/prism';
import {javascript} from 'react-syntax-highlighter/dist/esm/languages/prism';
import React, {useState} from "react";
import {styled} from "@mui/material/styles";
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import {IconButton, Textarea} from "theme-ui";
import DownloadIcon from '@mui/icons-material/Download';
import DOMPurify from 'dompurify';
import {materialTheme as theme} from "../Theme";
import {fetchSnapshotsPath} from "../utils/PlakarApiClient";
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined';


const fadeIn = keyframes`
  from {
    opacity: 0;
  }
  to {
    opacity: 1;
  }
`;

const fadeOut = keyframes`
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
`;


const FloatingBox = styled(Stack)(({theme}) => ({
    position: 'absolute',
    top: 32,
    right: 0,
    opacity: 0,
    animation: `${fadeOut} 0.5s forwards`,
    transition: 'opacity 0.5s',
    '&:hover': {
        animation: `${fadeIn} 0.5s forwards`,
    },
}));

const ConfirmationAlert = styled(Alert)(({theme}) => ({
    opacity: 0,
    visibility: 'hidden',
    transition: 'visibility 0s 6s, opacity 6s linear',
    '&.fade-in': {
        animation: `${fadeIn} 1s forwards`,
        visibility: 'visible',
    },
    '&.fade-out': {
        animation: `${fadeOut} 1s forwards`,
        visibility: 'visible',
        transition: 'visibility 0s 1s, opacity 1s linear',
    },
}));


const loadFile = (url = 'http://localhost:3000/demo-files/demo.js', callback) => {
    fetch(url)
        .then((r) => r.text())
        .then((rawText) => {
            const sanitizedContent = DOMPurify.sanitize(rawText);
            callback(sanitizedContent);
        });
}

// how to imple hightlighting
// https://blog.logrocket.com/guide-syntax-highlighting-react/

function FileDetails({snapshotId, path}) {
    let {id} = useParams();
    let [searchParams, setSearchParams] = useSearchParams();
    const [text, setText] = useState('Loading...');
    const [hovered, setHovered] = React.useState(false);
    const [visible, setVisible] = useState(false);
    const [showRaw, setShowRaw] = useState(false);
    const [fileData, setFileData] = useState(null);

    const handleDownloadClick = () => {
        const link = document.createElement('a');
        link.href = 'http://localhost:3000/demo-files/demo.js';
        link.download = 'demo.js';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    };

    const copyToClipboard = () => {
        navigator.clipboard.writeText(text);
        setVisible(true);
    }

    const handleRawClick = () => {
        setShowRaw(!showRaw);
        console.log('showRaw', showRaw)
    }


    React.useEffect(() => {
        fetchSnapshotsPath('', `${snapshotId}:${path}`, 1, 10).then((page) => {
            console.log('file data', page.items[0]);
            setFileData(page.items[0])
        });

        loadFile('http://localhost:3000/demo-files/demo.js', setText);
        let timeoutId;
        if (visible) {
            console.log('notification should go visible')
            // Hide after 6 seconds
            timeoutId = setTimeout(() => {
                setVisible(false);
            }, 1000);
        }
        return () => {
            clearTimeout(timeoutId);
        };
    }, [loadFile, visible]);

    return (<>
            <Typography variant="h3" component="h1">{fileData ? fileData.name : <Skeleton/>}</Typography>
            {id}
            {searchParams.get('p')}
            <FileBreadcrumbs path={path} snapshotid={snapshotId}/>

            {fileData && fileData.mimeType !== 'text' &&
                <Card variant="outlined" sx={{width: '424px', height: '224px', boxShadow: "0px 25px 50px 0px rgba(31, 41, 55, 0.25)", border: 0, borderRadius: 2}}>
                    <CardContent>
                        <Stack alignItems={'center'} spacing={1} pt={1}>
                            <InfoOutlinedIcon/>
                            <Typography variant="textlgmedium" component="h2">Preview unvailable</Typography>
                            <Typography variant='textsmregular' color={theme.palette.gray[500]}>Sorry, we don’t know how to interpret this file.</Typography>
                        </Stack>

                    </CardContent>
                    <CardActions>
                        <Stack sx={{flex: 1}} alignItems='center'>
                            <Button size="large" color="primary" variant={'contained'} endIcon={<DownloadIcon/>}>
                                <Typography variant={'textsmregular'}>Download Raw File</Typography>
                            </Button>
                        </Stack>
                    </CardActions>
                </Card>
            }

            <Stack sx={{
                position: 'relative', width: '100%', flex: 1

            }}
                   onMouseEnter={() => setHovered(true)}
                   onMouseLeave={() => setHovered(false)}
            >
                <FloatingBox className="floating-component" aria-label="add" size="small"
                             sx={{
                                 ...(hovered && {
                                     opacity: 1, animation: 'none',
                                 }),
                             }}>
                    <Stack alignItems='center' padding={1}>
                        <Stack direction='row'
                               width={'200px'}
                               p={1}
                               borderRadius={1}
                               sx={{backgroundColor: theme.palette.gray['600']}}>
                            <Tooltip title={showRaw ? "Toggle to Formatted" : "Toggle to Raw"}>
                                <Button sx={{color: 'white'}} onClick={handleRawClick}>Raw</Button>
                            </Tooltip>
                            <Box sx={{borderRight: '2px solid', borderLeft: '1px solid'}}>
                                <Tooltip title={"Copy to Clipboard"} placement="top">
                                    <Button sx={{color: 'white'}} onClick={copyToClipboard}><ContentCopyIcon/></Button>
                                </Tooltip>
                            </Box>
                            <Tooltip title={"Download File"}>
                                <Button sx={{color: 'white'}} onClick={handleDownloadClick}><DownloadIcon/></Button>
                            </Tooltip>
                        </Stack>
                    </Stack>
                    <ConfirmationAlert severity="success" color="info" className={visible ? 'fade-in' : 'fade-out'}>
                        Content Copied to Clipboard
                    </ConfirmationAlert>
                </FloatingBox>
                <Stack sx={{flex: 1}} padding={1}>
                    <Stack sx={{flex: 1, overflowY: 'auto', maxHeight: 'calc(100vh - 136px)'}} padding={1}>
                        {showRaw &&

                            <pre style={{margin: 0}}>{text}</pre>

                        }
                        {!showRaw &&
                            // <Stack  backgroundColor={"#FF4444"}>
                            <SyntaxHighlighter
                                // customStyle={{'flex-grow': 1, 'overflow-y': 'auto', 'display': 'flex'}}
                                showLineNumbers={true}
                                language="javascript"
                                style={a11yDark}>
                                {text}
                            </SyntaxHighlighter>
                            // </Stack>
                        }
                    </Stack>
                </Stack>
            </Stack>
        </>
    )
}


export default FileDetails;