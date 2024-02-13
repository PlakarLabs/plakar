import {styled} from "@mui/material/styles";
import TableRow from "@mui/material/TableRow";

const StyledTableRow = styled(TableRow)(({theme}) => ({
    '&:nth-of-type(odd)': {
        backgroundColor: 'white',
    },
    // hide lines for all
    '& td, & th': {
        border: 0,
    },
    "&:hover": {
        backgroundColor: theme.palette.gray[50],
    },

}));

export default StyledTableRow;
