// basic react function for a component

import React from 'react';
import {Typography, Stack, Link, Skeleton} from '@mui/material';

import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Paper from '@mui/material/Paper';
import TableFooter from '@mui/material/TableFooter';
import {materialTheme} from "../Theme";
import StyledTableCell from "../components/StyledTableCell";
import StyledTableRow from "../components/StyledTableRow";
import {Link as RouterLink} from "react-router-dom";
import StyledPagination from "../components/StyledPagination";
import {ReactComponent as FolderIcon} from '../icons/folder.svg';
import {ReactComponent as FileIcon} from '../icons/file.svg';
import FileBreadcrumbs from "../components/FileBreadcrumb";
import {useDispatch, useSelector} from "react-redux";
import {fetchPath, selectPathPage} from "../state/Root";


function PathList({snapshotId, path}) {
    const dispatch = useDispatch();
    const page = selectPathPage(useSelector(state => state));

    return (
        <>
            <Stack spacing={1} py={2}>

                {page.snapshot ?
                    <Stack direction={'row'} spacing={1} alignItems={'center'}>
                        <Typography variant="h3" component="h1">Snapshot</Typography>
                        <Link component={RouterLink}
                              to={`/snapshot/${page.snapshot.id}:${page.snapshot.rootPath}/`}>
                            <Typography variant="h3" component="h1">{page.snapshot.shortId}</Typography>
                        </Link>

                    </Stack>
                    :
                    <Skeleton width={'300px'}/>
                }

                {page.snapshot ? <FileBreadcrumbs path={path} snapshotid={page.snapshot.id}/> : <Skeleton width={'300px'}/>}
            </Stack>

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
                                <StyledPagination pageCount={page.totalPages} onChange={(event, page) => {
                                    dispatch(fetchPath({snapshotId, path, page, pageSize: 10}));
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