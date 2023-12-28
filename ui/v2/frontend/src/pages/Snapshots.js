// basic react function for a component

import React, {useEffect, useState} from 'react';
import {Typography, Stack, Link} from '@mui/material';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Paper from '@mui/material/Paper';
import TableFooter from '@mui/material/TableFooter';

import SingleScreenLayout from "../layouts/SingleScreenLayout";
import {materialTheme} from "../Theme";
import {fetchSnapshots, selectApiUrl, selectSnapshotsPage} from "../state/Root";
import {
    Link as RouterLink, useNavigate,
    useSearchParams
} from "react-router-dom";
import TagList from "../components/TagList";
import StyledTableCell from "../components/StyledTableCell";
import StyledTableRow from "../components/StyledTableRow";
import StyledPagination from "../components/StyledPagination";
import {shallowEqual, useDispatch, useSelector} from "react-redux";
import SearchBar from "../components/SearchBar";
import {snapshotURL} from "../utils/Routes";


function Snapshots() {
    const dispatch = useDispatch();
    let [searchParams, setSearchParams] = useSearchParams();
    const navigate = useNavigate();
    const page = useSelector(selectSnapshotsPage, shallowEqual);
    const [pageOffset, setPageOffset] = useState(1);
    const [pageSize, setPageSize] = useState(10);
    const apiUrl = useSelector(selectApiUrl, shallowEqual)
    let [searchQuery, setSearchQuery] = useState('');


    useEffect(() => {
        if (searchParams.get('page') !== pageOffset.toString()) {
            setPageOffset(parseInt(searchParams.get('page')));
            return;
        }
        if (searchParams.get('pageSize') !== pageSize.toString()) {
            setPageSize(parseInt(searchParams.get('pageSize')));
            return;
        }
        console.log('refresh');
        dispatch(fetchSnapshots(apiUrl, pageOffset, pageSize));
    }, [dispatch, setPageOffset, searchParams, setSearchParams, apiUrl, pageOffset, pageSize]);

    const handlePageChange = (event, page) => {
        setPageOffset(page);
        setSearchParams({page: page, pageSize: pageSize});

    }

    const onSearch = (searchQuery) => {
        navigate(`/search?q=${searchQuery}`);
    }

    return (
        <SingleScreenLayout>
            <Stack spacing={1}>
                <SearchBar onSearch={onSearch} setInputState={setSearchQuery} inputState={searchQuery}/>
                <Typography variant="h3" component="h1">Snapshots</Typography>
                <TableContainer component={Paper}>
                    <Table sx={{minWidth: 700}} size="small" aria-label="customized table">
                        <TableHead>
                            <TableRow>
                                <StyledTableCell>
                                    <Typography variant={"textxsmedium"} color={materialTheme.palette.gray['600']}>
                                        Snapshot
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
                            {page && page.items.map((row) => (
                                <StyledTableRow key={row.id}>
                                    <StyledTableCell component="th" scope="row" sx={{whiteSpace: 'nowrap'}}>
                                        <Link component={RouterLink} to={snapshotURL(row.id, row.rootPath + '/')}
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
                                    <StyledPagination page={pageOffset} pageCount={page ? page.totalPages : 0}
                                                      onChange={handlePageChange}/>
                                </td>
                            </TableRow>
                        </TableFooter>
                    </Table>
                </TableContainer>
            </Stack>

        </SingleScreenLayout>
    );

};

export default Snapshots;
