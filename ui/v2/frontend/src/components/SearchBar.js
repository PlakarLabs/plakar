import React from 'react';
import {InputAdornment, InputBase, Stack, Tooltip, Typography} from "@mui/material";
import SearchIcon from "@mui/icons-material/Search";
import KeyboardCommandKeyIcon from '@mui/icons-material/KeyboardCommandKey';
import {materialTheme} from "../Theme";
import {useEffect, useRef} from "react";
import WindowsIcon from "./WindowsIcon";
import {getOS, OS} from "../utils/BrowserInteraction";

const SearchBar = ({onSearch = null, inputState = null, setInputState= null}) => {
    const inputRef = useRef(null);

    useEffect(() => {
        const handleKeyDown = (event) => {
            if (event.metaKey && event.key.toLowerCase() === 'k') {
                console.log('search press');
                event.preventDefault();
                inputRef.current.focus();
            }
            if (event.key === "Enter") {
                event.preventDefault();
                console.log('return press', inputRef.current.value);
                if (onSearch) {
                    onSearch(inputRef.current.value);
                }
                // perform search
            }


        };

        document.addEventListener('keydown', handleKeyDown);

        return () => {
            document.removeEventListener('keydown', handleKeyDown);
        };
    }, [onSearch, inputState]);

    return (
        <Stack direction={'row'} sx={{border: 2, borderColor: materialTheme.palette.gray['100'], borderRadius: 2}}
               padding={1} spacing={1} alignItems={'center'}>
            <SearchIcon color={'primary'}/>
            <InputBase
                inputRef={inputRef}
                fullWidth
                placeholder="Search..."
                value={inputState}
                id="search"
                onChange={(event) => { if (setInputState) { setInputState(event.target.value); }}}
                sx={{borderRadius: 1}}
                endAdornment={
                    <InputAdornment position="end">
                        <Tooltip title="Command+K" enterDelay={500}>
                            <Stack direction={'row'} alignItems={'center'} sx={{
                                backgroundColor: materialTheme.palette.primary.main,
                                border: 1,
                                borderColor: materialTheme.palette.gray['100'],
                                borderRadius: 2
                            }} padding={1}>
                                {(() => {
                                    switch (getOS()) {
                                        case OS.MAC:
                                            return <KeyboardCommandKeyIcon sx={{fontSize: 14, color: 'white'}}
                                                                           style={{cursor: 'help'}}/>
                                        case OS.WINDOWS:
                                            return <WindowsIcon variant={'primary'} fontSize="tiny"
                                                                style={{cursor: 'help'}}/>
                                        default:
                                            return <></>
                                    }
                                })()}
                                <Typography variant={'textxsmedium'}
                                            color={materialTheme.palette.gray['100']}>K</Typography>
                            </Stack>
                        </Tooltip>
                    </InputAdornment>}
            />
        </Stack>
    )
}

export default SearchBar;