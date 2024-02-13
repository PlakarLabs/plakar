import React from 'react';

import Tag from "./Tag";
import {Stack} from "@mui/material";

function TagList({tags = []}) {
    tags = tags == null ? [] : tags;
    return (
        <Stack direction={'row'} spacing={1} py={1} useFlexGap flexWrap="wrap">
            { tags.map((tag) => (
                <Tag key={tag} text={tag}/>
            ))}
        </Stack>
    );
}

export default TagList;
