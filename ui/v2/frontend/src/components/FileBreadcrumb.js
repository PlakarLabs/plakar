import {Breadcrumbs, Link, Typography} from "@mui/material";
import {getFolderNameAndPathPairs} from "../utils/Path";
import {Link as RouterLink} from "react-router-dom";
import React from "react";
import NavigateNextIcon from '@mui/icons-material/NavigateNext';

function FileBreadcrumbs({snapshotid, path}) {
    return (
        <>
            <Breadcrumbs color={'primary'} aria-label="breadcrumb">
                    {path && getFolderNameAndPathPairs(path).map(({name, path}) => {
                        return <Link key={name} component={RouterLink} underline="hover"
                                     to={`/snapshot/${snapshotid}:${path}`}>
                            {name}
                        </Link>

                    })}
                    {/*<Typography color="text.primary">/</Typography>*/}
                </Breadcrumbs>
        </>
    );
}

export default FileBreadcrumbs;