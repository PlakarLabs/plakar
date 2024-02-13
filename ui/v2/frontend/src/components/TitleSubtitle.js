import {Skeleton, Stack, Typography} from "@mui/material";
import React from "react";

function TitleSubtitle({title= null, subtitle=null}) {
    return (
        <Stack>
            <Typography variant={'textbasemedium'}>{title ? title : <Skeleton/>}</Typography>
            <Typography variant={'textsmregular'}>{subtitle ? subtitle : <Skeleton/>}</Typography>
        </Stack>
    )
}

export default TitleSubtitle