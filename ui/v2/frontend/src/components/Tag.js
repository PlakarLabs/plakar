import React from 'react';
import {Chip} from "@mui/material";

const Tag = ({text}) => {
    return (<Chip variant={'tag'} size={'small'} label={`# ${text}`}/>
    );
};

export default Tag;
