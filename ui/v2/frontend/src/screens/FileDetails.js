import {useParams, useSearchParams} from "react-router-dom";
import FileBreadcrumbs from "../components/FileBreadcrumb";
import {Typography} from "@mui/material";
import {Prism as SyntaxHighlighter} from 'react-syntax-highlighter';
import {dark} from 'react-syntax-highlighter/dist/esm/styles/prism';
import { javascript } from 'react-syntax-highlighter/dist/esm/languages/prism';
import {useState} from "react";




// how to imple hightlighting
// https://blog.logrocket.com/guide-syntax-highlighting-react/

function FileDetails() {
    let {id} = useParams();
    let [searchParams, setSearchParams] = useSearchParams();
    const [text, setText] = useState('const fred = 1;');


    return (
        <div>
            <h1>Details</h1>
            <Typography variant={'h2'}>1f4b8595</Typography>
            {id}
            {searchParams.get('p')}
            <FileBreadcrumbs file={'/asd/asd'} snapshotid={'abc'}/>

            <SyntaxHighlighter language="javascript" style={dark}>
                {text}
            </SyntaxHighlighter>
        </div>
    )
}



export default FileDetails;