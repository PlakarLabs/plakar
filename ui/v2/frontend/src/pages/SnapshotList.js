// basic react function for a component

import React from 'react';
import {connect} from 'react-redux';
import {useState, useEffect} from 'react';
import {useDispatch, useSelector} from 'react-redux';
import {selectSnapshots} from '../state/Root';
import {Typography, Stack, AppBar, Container, TextField, Box, InputBase, Pagination, Link} from '@mui/material';
import {styled} from '@mui/material/styles';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell, {tableCellClasses} from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Paper from '@mui/material/Paper';
import TableFooter from '@mui/material/TableFooter';
import TablePagination from '@mui/material/TablePagination';
import InputAdornment from '@mui/material/InputAdornment';

import Tag from '../components/Tag'
import DefaultLayout from "../layouts/DefaultLayout";
import SingleScreenLayout from "../layouts/SingleScreenLayout";
import {IconButton} from "theme-ui";
import SearchIcon from '@mui/icons-material/Search';
import {materialTheme} from "../Theme";
import {fetchSnapshots, snapshots} from "../utils/PlakarApiClient";
import {Link as RouterLink} from "react-router-dom";
import TagList from "../components/TagList";
import StyledTableCell from "../components/StyledTableCell";
import StyledTableRow from "../components/StyledTableRow";
import StyledPagination from "../components/StyledPagination";


function SnapshotList({}) {
    const [page, setPage] = React.useState(fetchSnapshots('', 1, 10));

    // useEffect(() => {
    //         fetchSnapshots();
    //
    //     },
    //
    //     [fetchSnapshots]);


    return (
        <SingleScreenLayout>
            <Stack spacing={1}>
                <TextField fullWidth
                           label="Search..."
                           id="search"
                           sx={{boxShadow: 3, borderRadius: 1}}
                           InputProps={{
                               endAdornment: (
                                   <InputAdornment position="start">
                                       <SearchIcon/>
                                   </InputAdornment>
                               ),
                           }}/>
                <Typography variant="h3" component="h1">Snapshots</Typography>
                <TableContainer component={Paper}>
                    <Table sx={{minWidth: 700}} size="small" aria-label="customized table">
                        <TableHead>
                            <TableRow>
                                <StyledTableCell>
                                    <Typography variant={"textxsmedium"} color={materialTheme.palette.gray['600']}>
                                        Snapshot Id
                                    </Typography>
                                </StyledTableCell>
                                <StyledTableCell align="right">
                                    <Typography variant={"textxsmedium"} color={materialTheme.palette.gray['600']}>
                                        Username
                                    </Typography>
                                </StyledTableCell>
                                <StyledTableCell align="right">
                                    <Typography variant={"textxsmedium"} color={materialTheme.palette.gray['600']}>
                                        Hostname
                                    </Typography>
                                </StyledTableCell>
                                <StyledTableCell align="right">
                                    <Typography variant={"textxsmedium"} color={materialTheme.palette.gray['600']}>
                                        Date
                                    </Typography>
                                </StyledTableCell>
                                <StyledTableCell align="right">
                                    <Typography variant={"textxsmedium"} color={materialTheme.palette.gray['600']}>
                                        Size
                                    </Typography>
                                </StyledTableCell>
                                <StyledTableCell align="right">
                                    <Typography variant={"textxsmedium"} color={materialTheme.palette.gray['600']}>
                                        Tags
                                    </Typography>
                                </StyledTableCell>
                            </TableRow>
                        </TableHead>
                        <TableBody>
                            {page.items.map((row) => (
                                <StyledTableRow key={row.id}>
                                    <StyledTableCell component="th" scope="row" sx={{whiteSpace: 'nowrap'}}>
                                        <Link component={RouterLink} to={`/snapshot/${row.id}:${row.rootPath}/`}
                                              underline={'none'} variant={'primary'}>
                                            <Typography variant='textsmregular'>{row.shortId}</Typography>
                                        </Link>
                                    </StyledTableCell>
                                    <StyledTableCell align="right" sx={{whiteSpace: 'nowrap'}}>
                                        <Typography variant='textsmregular'>{row.username}</Typography>
                                    </StyledTableCell>
                                    <StyledTableCell align="right" sx={{whiteSpace: 'nowrap'}}>
                                        <Typography variant='textsmregular'>{row.hostName}</Typography>
                                    </StyledTableCell>
                                    <StyledTableCell sx={{whiteSpace: 'nowrap'}} align="right">
                                            <Typography variant='textsmregular'>{row.date}</Typography>
                                    </StyledTableCell>
                                    <StyledTableCell align="right" sx={{whiteSpace: 'nowrap'}}>
                                        <Typography variant='textsmregular'>{row.size}</Typography>
                                    </StyledTableCell>
                                    <StyledTableCell align="right" sx={{maxWidth: '300px'}}>
                                        <TagList tags={row.tags}/>
                                    </StyledTableCell>
                                </StyledTableRow>
                            ))}
                        </TableBody>
                        <TableFooter>
                            <TableRow>
                                <td colSpan={10}>
                                    <StyledPagination pageCount={page.totalPages} onChange={(event, page) => {
                                        setPage(fetchSnapshots('', page, 10));
                                    }}/>
                                </td>
                            </TableRow>
                        </TableFooter>
                    </Table>
                </TableContainer>
            </Stack>

        </SingleScreenLayout>
    );

};

export default SnapshotList;