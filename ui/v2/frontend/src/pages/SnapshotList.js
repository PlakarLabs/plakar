// basic react function for a component

import React from 'react';
import {connect} from 'react-redux';
import {useState, useEffect} from 'react';
import {useDispatch, useSelector} from 'react-redux';
import {selectSnapshots} from '../state/Root';
import {Typography, Stack, AppBar, Container, TextField, Box, InputBase, Pagination} from '@mui/material';
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


const StyledTableCell = styled(TableCell)(({theme}) => ({
    [`&.${tableCellClasses.head}`]: {
        backgroundColor: theme.palette.grey['50'],
    },
    [`&.${tableCellClasses.body}`]: {
        // fontSize: 14,
        // lineHeight: '20px',
    },
}));

// change the color for intermediate rows if needed
const StyledTableRow = styled(TableRow)(({theme}) => ({
    '&:nth-of-type(odd)': {
        backgroundColor: 'white',
    },
    // hide lines for all
    '& td, & th': {
        border: 0,
    },
}));

function createData(
    name,
    calories,
    fat,
    carbs,
    protein,
) {
    return {name, calories, fat, carbs, protein};
}

const rows = fetchSnapshots('', 1, 10).items;


function SnapshotList({}) {
    const [page, setPage] = React.useState(0);

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
                           sx={{ boxShadow: 3, borderRadius: 1 }}
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
                                        Size</Typography>
                                </StyledTableCell>
                            </TableRow>
                        </TableHead>
                        <TableBody>
                            {rows.map((row) => (
                                <StyledTableRow key={row.name}>
                                    <StyledTableCell component="th" scope="row">
                                        <Typography variant='textsmregular'>{row.id}</Typography>
                                    </StyledTableCell>
                                    <StyledTableCell align="right">
                                        <Typography variant='textsmregular'>{row.username}</Typography>
                                    </StyledTableCell>
                                    <StyledTableCell align="right">
                                        <Typography variant='textsmregular'>{row.hostName}</Typography>
                                    </StyledTableCell>
                                    <StyledTableCell align="right">
                                        <Typography variant='textsmregular'>{row.date}</Typography>
                                    </StyledTableCell>
                                    <StyledTableCell align="right">
                                        <Typography variant='textsmregular'>{row.size}</Typography>
                                    </StyledTableCell>
                                </StyledTableRow>
                            ))}
                        </TableBody>
                        <TableFooter>
                            <TableRow>
                                <td colSpan={10}>
                                    <Stack sx={{width: "100%"}} alignItems="stretch" direction={'row'}
                                           justifyContent={"flex-start"} padding={2}>
                                        <Pagination count={10} color={'primary'} size={'small'}/>

                                        {/*<TablePagination*/}

                                        {/*    showFirstButton*/}
                                        {/*    showLastButton*/}
                                        {/*    rowsPerPageOptions={[5, 10, 25, {label: 'All', value: -1}]}*/}
                                        {/*    colSpan={3}*/}
                                        {/*    count={rows.length}*/}
                                        {/*    rowsPerPage={10}*/}
                                        {/*    page={page}*/}
                                        {/*    SelectProps={{*/}
                                        {/*        inputProps: {*/}
                                        {/*            'aria-label': 'rows per page',*/}
                                        {/*        },*/}
                                        {/*        native: true,*/}
                                        {/*    }}*/}

                                        {/*/>*/}
                                    </Stack>
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