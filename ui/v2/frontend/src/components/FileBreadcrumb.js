import React from "react";

import {Breadcrumbs, Link, Stack, Typography} from "@mui/material";
import {getFolderNameAndPathPairs} from "../utils/Path";
import {Link as RouterLink} from "react-router-dom";

import {snapshotURL} from "../utils/Routes";

function FileBreadcrumbs({snapshotid, path}) {
    return (
        <>
            <Stack alignItems={'top'} direction={'row'} spacing={1}>
                <Typography color="text.primary">/</Typography>
                <Breadcrumbs color={'primary'} aria-label="breadcrumb">
                    {path && getFolderNameAndPathPairs(path).map(({name, path}) => {
                        return (
                        <Link key={name} component={RouterLink} underline="hover"
                            to={snapshotURL(snapshotid, path)}>{name}</Link>
                        );
                    })}
                </Breadcrumbs>
            </Stack>
        </>
    );
}

export default FileBreadcrumbs;