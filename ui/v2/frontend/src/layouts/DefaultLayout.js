import {AppBar, Container, Stack, Typography} from "@mui/material";
import React from "react";
import CssBaseline from "@mui/material/CssBaseline";
import {ThemeUIProvider} from 'theme-ui';
import {
    THEME_ID,
    ThemeProvider as MaterialThemeProvider
} from '@mui/material/styles';
import {materialTheme, themeUITheme} from '../Theme';
import {connect} from "react-redux";
import {confApp, selectConf} from "../state/Root";
import { ReactComponent as Logo } from '../Logo/Full.svg';

function DefaultLayout({children, conf}) {

    return (
        <>
            <CssBaseline/>
            <ThemeUIProvider theme={themeUITheme}>
                <MaterialThemeProvider theme={{[THEME_ID]: materialTheme}}>
                    <Stack>
                        <AppBar position="static" color="transparent">
                            <Stack direction={{xs: 'column', sm: 'row'}}
                                   justifyContent="left"
                                   alignItems="center"
                                   maxWidth="xl" sx={{padding: 2}}>
                                <Logo padding="s"/>
                                <Typography href="#"
                                            sx={{padding: 1}}>on <strong>{conf.storeName ? conf.storeName : 'loading...'}</strong></Typography>
                            </Stack>
                        </AppBar>
                            {children}
                    </Stack>
                </MaterialThemeProvider>
            </ThemeUIProvider>
        </>
    );
}


const mapStateToProps = state => ({
    conf: selectConf(state),
});

const mapDispatchToProps = {
    confApp,
};

export default connect(mapStateToProps, mapDispatchToProps)(DefaultLayout);