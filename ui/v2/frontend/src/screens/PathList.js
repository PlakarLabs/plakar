// basic react function for a component

import React from 'react';
import {useState, useEffect} from 'react';
import {Typography, Stack, AppBar, Container, Link, Breadcrumbs} from '@mui/material';

import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Paper from '@mui/material/Paper';
import TableFooter from '@mui/material/TableFooter';
import TablePagination from '@mui/material/TablePagination';
import {fetchSnapshots, fetchSnapshotsPath} from "../utils/PlakarApiClient";
import {getFolderNameAndPathPairs} from "../utils/Path";
import {materialTheme} from "../Theme";
import StyledTableCell from "../components/StyledTableCell";
import StyledTableRow from "../components/StyledTableRow";
import {Link as RouterLink} from "react-router-dom";
import StyledPagination from "../components/StyledPagination";
import {ReactComponent as FolderIcon} from '../icons/folder.svg';
import {ReactComponent as FileIcon} from '../icons/file.svg';
import FileBreadcrumbs from "../components/FileBreadcrumb";


function PathList({snapshotId, path}) {
    const [page, setPage] = React.useState(null);
    const [splittedPath, setSplittedPath] = React.useState([]);
    // let {id, path} = useParams();

    useEffect(() => {
            let pathId = `${snapshotId}:${path}`;
            fetchSnapshotsPath('', pathId, 1, 10).then((newPage) => {
                setPage(newPage)
            });
            setSplittedPath(getFolderNameAndPathPairs(path))

        },

        [fetchSnapshotsPath]);


    return (
        <>
        <Stack spacing={1} py={2}>

            {page && <Typography variant="h3" component="h1">Snapshot <Link component={RouterLink}
                                                                            to={'page.snapshot.rootPath'}>{page && page.snapshot.shortId}</Link>
            </Typography>}

            {page && <FileBreadcrumbs path={path} snapshotid={page.snapshot.id}/>}
        </Stack>
        {/*<Typography>{path}</Typography>*/
        }

        <TableContainer component={Paper}>
            <Table sx={{minWidth: 700}} aria-label="customized table">
                <TableHead>
                    <TableRow>
                        <StyledTableCell><Typography variant={"textxsmedium"}
                                                     color={materialTheme.palette.gray['600']}>
                            Path
                        </Typography>
                        </StyledTableCell>
                        <StyledTableCell>
                            <Typography variant={"textxsmedium"}
                                        color={materialTheme.palette.gray['600']}>
                                Mode
                            </Typography>
                        </StyledTableCell>

                        <StyledTableCell align="right"><Typography variant={"textxsmedium"}
                                                                   color={materialTheme.palette.gray['600']}>
                            Uid
                        </Typography></StyledTableCell>
                        <StyledTableCell align="right"><Typography variant={"textxsmedium"}
                                                                   color={materialTheme.palette.gray['600']}>
                            Gid
                        </Typography></StyledTableCell>
                        <StyledTableCell align="right"><Typography variant={"textxsmedium"}
                                                                   color={materialTheme.palette.gray['600']}>
                            Date
                        </Typography></StyledTableCell>
                        <StyledTableCell align="right"><Typography variant={"textxsmedium"}
                                                                   color={materialTheme.palette.gray['600']}>
                            Size
                        </Typography></StyledTableCell>
                    </TableRow>
                </TableHead>
                <TableBody>
                    {page && page.items.map((row) => (
                        <StyledTableRow key={row.path}>
                            <StyledTableCell align="left">
                                <Link underline='none' component={RouterLink}
                                      to={`/snapshot/${row.path}`}>
                                <Stack direction={'row'} spacing={1} alignItems={'center'}>
                                    {row.isDirectory && <FolderIcon/>}
                                    {!row.isDirectory && <FileIcon/>}
                                    <Typography
                                        variant='textsmregular'>{row.name}
                                    </Typography>
                                </Stack>
                            </Link>
                        </StyledTableCell>
                        <StyledTableCell align="left"><Typography
                        variant='textsmregular'>{row.mode}</Typography></StyledTableCell>
            <StyledTableCell align="right"><Typography
                variant='textsmregular'>{row.uid}</Typography></StyledTableCell>
            <StyledTableCell align="right"><Typography
                variant='textsmregular'>{row.gid}</Typography></StyledTableCell>
            <StyledTableCell align="right"><Typography
                variant='textsmregular'>{row.date}</Typography></StyledTableCell>
            <StyledTableCell align="right"><Typography
                variant='textsmregular'>{row.size}</Typography></StyledTableCell>
        </StyledTableRow>
        ))}
        </TableBody>
    <TableFooter>
        <TableRow>
            <td colSpan={10}>
                <StyledPagination pageCount={1} onChange={(event, page) => {
                    setPage(fetchSnapshots('', page, 10));
                }}/>
            </td>
        </TableRow>
    </TableFooter>
</Table>
</TableContainer>


</>
)
    ;

};

export default PathList;