import {Link, Stack, Typography} from "@mui/material";
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
import {Link as RouterLink} from "react-router-dom";
import TableFooter from "@mui/material/TableFooter";
import StyledPagination from "../components/StyledPagination";
import SingleScreenLayout from "../layouts/SingleScreenLayout";
import React from "react";

const SearchResults = () => {
    return (
        <SingleScreenLayout>
            <Stack spacing={1}>
                <SearchBar/>
                <Typography variant="h3" component="h1">Search Results for "xxxx"</Typography>
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
                            {[{id: 1}].map((row) => (
                                <StyledTableRow key={row.id}>
                                    <StyledTableCell component="th" scope="row" sx={{whiteSpace: 'nowrap'}}>
                                        <Link component={RouterLink} to={`/snapshot/${row.id}:${row.rootPath}/`}
                                              underline={'none'} variant={'primary'}>
                                            <Typography variant='textsmregular'>xxx</Typography>
                                        </Link>
                                    </StyledTableCell>
                                    <StyledTableCell align="right" sx={{whiteSpace: 'nowrap'}}>
                                        <Typography variant='textsmregular'>xxx</Typography>
                                    </StyledTableCell>
                                    <StyledTableCell align="right" sx={{whiteSpace: 'nowrap'}}>
                                        <Typography variant='textsmregular'>xxx</Typography>
                                    </StyledTableCell>
                                    <StyledTableCell sx={{whiteSpace: 'nowrap'}} align="right">
                                        <Typography variant='textsmregular'>xxx</Typography>
                                    </StyledTableCell>
                                </StyledTableRow>
                            ))}
                        </TableBody>
                        <TableFooter>
                            <TableRow>
                                <td colSpan={10}>
                                    <StyledPagination pageCount={0} />
                                </td>
                            </TableRow>
                        </TableFooter>
                    </Table>
                </TableContainer>
            </Stack>

        </SingleScreenLayout>
    );
}

export default SearchResults;