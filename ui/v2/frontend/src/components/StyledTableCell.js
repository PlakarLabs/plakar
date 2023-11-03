import {styled} from '@mui/material/styles';
import TableCell, {tableCellClasses} from '@mui/material/TableCell';

const StyledTableCell = styled(TableCell)(({theme}) => ({
    [`&.${tableCellClasses.head}`]: {
        backgroundColor: theme.palette.grey['50'],
    },
    [`&.${tableCellClasses.body}`]: {

        // fontSize: 14,
        // lineHeight: '20px',
    },
}));

export default StyledTableCell;