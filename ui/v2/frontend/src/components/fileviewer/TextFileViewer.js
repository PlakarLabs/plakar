import {
    Alert,
    Box,
    Button,
    keyframes,
    Stack,
    Tooltip,
} from "@mui/material";
import {Prism as SyntaxHighlighter} from 'react-syntax-highlighter';
import { dracula} from 'react-syntax-highlighter/dist/esm/styles/prism';
import React, {useCallback, useState} from "react";
import {styled} from "@mui/material/styles";
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import DownloadIcon from '@mui/icons-material/Download';
import DOMPurify from 'dompurify';
import {materialTheme as theme} from "../../Theme";
import {confApp, selectFileDetails} from "../../state/Root";
import {connect} from "react-redux";
import {triggerDownload, copyToClipboard} from "../../utils/BrowserInteraction";

import { useMemo } from 'react';
import { useSelector } from "react-redux";
import { useParams } from "react-router-dom";

import { prepareParams } from "../../pages/Explorer";
import { lookupFileDetails } from "../../state/Root";

import Prism from "prismjs";
//import style from "react-syntax-highlighter/dist/esm/styles/hljs/dracula";


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


const loadFile = (url, callback) => {
    fetch(url)
        .then((r) => r.text())
        .then((rawText) => {
            const sanitizedContent = DOMPurify.sanitize(rawText);
            callback(sanitizedContent);
        });
}

function TextFileViewer() {
    const params = useParams();
    const { snapshotId, path } = useMemo(() => prepareParams(params), [params]);
    const fileDetails = useSelector(state => lookupFileDetails(state, snapshotId + ":" + path));


    //const [text, setText] = useState('Loading...');
    const [text, setText] = useState('');
    const [hovered, setHovered] = React.useState(false);
    const [visible, setVisible] = useState(false);
    const [showRaw, setShowRaw] = useState(false);

    const handleDownloadClick = useCallback(() => {
        triggerDownload(fileDetails.rawPath + "?download=true", fileDetails.name);
    }, [fileDetails.rawPath, fileDetails.name]);

    const handleCopyToClipboard = () => {
        copyToClipboard(text);        
    }

    const handleRawClick = () => {
        setShowRaw(!showRaw);
    }


    React.useEffect(() => {
        
        Prism.highlightAll();
          
        loadFile(fileDetails.rawPath, setText);
        let timeoutId;
        if (visible) {
            // Hide after 6 seconds
            timeoutId = setTimeout(() => {
                setVisible(false);
            }, 1000);
        }
        return () => {
            clearTimeout(timeoutId);
        };
    }, [visible, fileDetails.rawPath]);

    return (<>
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
                                    <Button sx={{color: 'white'}} onClick={handleCopyToClipboard}><ContentCopyIcon/></Button>
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
                                language={fileDetails.name.split('.')[1]}
                                style={dracula}>
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

const mapStateToProps = state => ({
    fileDetails: selectFileDetails(state),
});

const mapDispatchToProps = {
    confApp,
};

export default connect(mapStateToProps, mapDispatchToProps)(TextFileViewer);
