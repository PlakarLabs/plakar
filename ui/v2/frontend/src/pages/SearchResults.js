import {Link, Skeleton, Stack, Typography} from "@mui/material";
import SearchBar from "../components/SearchBar";
import TableContainer from "@mui/material/TableContainer";
import Paper from "@mui/material/Paper";
import Table from "@mui/material/Table";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import StyledTableCell from "../components/StyledTableCell";
import {materialTheme} from "../Theme";
import TableBody from "@mui/material/TableBody";
import StyledTableRow from "../components/StyledTableRow";
import {Link as RouterLink, useSearchParams} from "react-router-dom";
import TableFooter from "@mui/material/TableFooter";
import SingleScreenLayout from "../layouts/SingleScreenLayout";
import React, {useEffect, useState} from "react";
import {search, selectSearchLoading, selectSearchParams, selectSearchResult} from "../state/Root";
import {shallowEqual, useDispatch, useSelector} from "react-redux";
import {snapshotURL} from "../utils/Routes";

const SearchResults = () => {
    const dispatch = useDispatch();
    let [searchParams, setSearchParams] = useSearchParams();
    let [searchQuery, setSearchQuery] = useState('');

    const searchResult = useSelector(selectSearchResult, shallowEqual);
    const submittedSearchParams = useSelector(selectSearchParams, shallowEqual);
    const isSearchLoading = useSelector(selectSearchLoading, shallowEqual);


    const onSearch = (searchQuery) => {
        setSearchParams({q: searchQuery})
    }

    useEffect(() => {
        const query = searchParams.get('q');
        searchQuery !== query && setSearchQuery(query);
        dispatch(search(query));
    }, [dispatch, setSearchParams, searchParams, searchQuery]);


    return (
        <SingleScreenLayout>
            <Stack spacing={1}>
                <SearchBar query={submittedSearchParams} onSearch={onSearch} inputState={searchQuery} setInputState={setSearchQuery} />
                <Stack direction={'row'} alignItems={'center'} spacing={1}>
                    <Typography variant="h3" component="h1">Search Results</Typography>
                    {submittedSearchParams && submittedSearchParams !== '' &&
                        <>
                            <Typography variant="textsmregular" component="h1">for</Typography>
                            <Typography variant="textlgmedium" component="h1">"{submittedSearchParams}"</Typography>
                        </>
                    }
                </Stack>

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
                                        Date
                                    </Typography>
                                </StyledTableCell>
                                <StyledTableCell align="right">
                                    <Typography variant={"textxsmedium"} color={materialTheme.palette.gray['600']}>
                                        Type
                                    </Typography>
                                </StyledTableCell>
                                <StyledTableCell align="right">
                                    <Typography variant={"textxsmedium"} color={materialTheme.palette.gray['600']}>
                                        Path
                                    </Typography>
                                </StyledTableCell>
                            </TableRow>
                        </TableHead>
                        <TableBody>
                            {isSearchLoading &&
                            <StyledTableRow key={'loading'}>
                                <StyledTableCell component="th" scope="row" sx={{whiteSpace: 'nowrap'}}>
                                    <Skeleton />
                                </StyledTableCell>
                            </StyledTableRow>}
                            {searchResult && searchResult.map((row, index) => (
                                <StyledTableRow key={`search-item-${index}`}>
                                    <StyledTableCell component="th" scope="row" sx={{whiteSpace: 'nowrap'}}>
                                        <Link component={RouterLink} to={snapshotURL(row.snapshot.id, row.snapshot.rootPath)}
                                              underline={'none'} variant={'primary'}>
                                            <Typography variant='textsmregular'>{row.snapshot.shortId}</Typography>
                                        </Link>
                                    </StyledTableCell>
                                    <StyledTableCell align="right" sx={{whiteSpace: 'nowrap'}}>
                                        <Typography variant='textsmregular'>{row.date}</Typography>
                                    </StyledTableCell>
                                    <StyledTableCell align="right" sx={{whiteSpace: 'nowrap'}}>
                                        <Typography variant='textsmregular'>{row.type}</Typography>
                                    </StyledTableCell>
                                    <StyledTableCell sx={{whiteSpace: 'nowrap'}} align="right">
                                        <Link component={RouterLink} to={snapshotURL(row.snapshot.id, row.path)}
                                              underline={'none'} variant={'primary'}>
                                        <Typography variant='textsmregular'>{row.path}</Typography>
                                            </Link>
                                    </StyledTableCell>
                                </StyledTableRow>
                            ))}
                        </TableBody>
                        <TableFooter>
                            {/*<TableRow>*/}
                            {/*    <td colSpan={10}>*/}
                            {/*        <StyledPagination pageCount={0}/>*/}
                            {/*    </td>*/}
                            {/*</TableRow>*/}
                        </TableFooter>
                    </Table>
                </TableContainer>
            </Stack>

        </SingleScreenLayout>
    );
}

export default SearchResults;